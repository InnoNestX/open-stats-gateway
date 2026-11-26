const { Hono } = require('hono');
const { cors } = require('hono/cors');
const { Redis } = require('@upstash/redis');
const fs = require('fs');
const path = require('path');
const { checkRateLimit } = require('./lib/rate-limiter');
const { fetchDockerHub, createDockerClient } = require('./lib/docker-client');
const {
  buildUserStatsResponse,
  getTopRepositories,
  getUserStats
} = require('./lib/user-stats');
const { generateOpenApiSpec } = require('./lib/openapi');
const { buildShieldsBadge, buildUserBadges, formatCompact } = require('./lib/badges');
const {
  getRepoHistory,
  getUserHistory,
  listTrackedUsers,
  recordRepoSnapshot,
  recordUserSnapshot
} = require('./lib/history');
const {
  AppError,
  ValidationError,
  NotFoundError,
  RateLimitError
} = require('./lib/errors');
const {
  createDockerStatsSvgWithStyle,
  createRepoStatsSvgWithStyle,
  createTrendSvgWithStyle,
  getAvailableStyles,
  getUserFacingStatsError
} = require('./lib/svg-utils');

const app = new Hono();
const dockerClient = createDockerClient();
const STATS_CACHE_SECONDS = 120;
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || 'https://docker-hub-pull-counter.vercel.app';

const redis = process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN
  ? new Redis({
      url: process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN
    })
  : null;

const EMPTY_ENDPOINT_STATS = {
  'user/stats': 0,
  'docker-stats': 0,
  'repo/details': 0,
  'repo/tags': 0,
  search: 0,
  'batch/stats': 0,
  'popular/repos': 0,
  compare: 0,
  embed: 0,
  'user/top-repos': 0,
  'user/history': 0,
  'user/growth': 0,
  'repo/history': 0,
  trend: 0,
  openapi: 0,
  badge: 0,
  'internal/snapshot-history': 0
};

async function trackCall(endpoint) {
  if (!redis) return;

  try {
    const pipeline = redis.pipeline();
    pipeline.incr('stats:totalCalls');
    pipeline.incr(`stats:endpoint:${endpoint}`);
    pipeline.set('stats:lastUpdated', new Date().toISOString());
    await pipeline.exec();
  } catch (error) {
    console.error('Track call error:', error.message);
  }
}

async function getStats() {
  if (!redis) {
    return {
      totalCalls: 0,
      byEndpoint: { ...EMPTY_ENDPOINT_STATS },
      lastUpdated: new Date().toISOString(),
      warning: 'Redis not configured'
    };
  }

  try {
    const keys = [
      'stats:totalCalls',
      ...Object.keys(EMPTY_ENDPOINT_STATS).map((endpoint) => `stats:endpoint:${endpoint}`),
      'stats:lastUpdated'
    ];
    const values = await redis.mget(...keys);
    const byEndpoint = {};
    const endpointKeys = Object.keys(EMPTY_ENDPOINT_STATS);
    endpointKeys.forEach((endpoint, index) => {
      byEndpoint[endpoint] = Number(values[index + 1] || 0);
    });

    return {
      totalCalls: Number(values[0] || 0),
      byEndpoint,
      lastUpdated: values[values.length - 1] || new Date().toISOString()
    };
  } catch (error) {
    console.error('Redis error:', error.message);
    return {
      totalCalls: 0,
      byEndpoint: { ...EMPTY_ENDPOINT_STATS },
      lastUpdated: new Date().toISOString(),
      error: 'Failed to fetch stats'
    };
  }
}

function isNotFoundError(error) {
  return error instanceof NotFoundError || error.message === 'Resource not found';
}

function errorPayload(error, fallbackMessage) {
  if (error instanceof AppError) {
    return error.toJSON();
  }

  return {
    success: false,
    error: fallbackMessage || error.message,
    code: 'INTERNAL_ERROR',
    originalError: error.message
  };
}

function cacheHeaders(source) {
  if (source === 'cache') {
    return {
      'Cache-Control': `public, max-age=${STATS_CACHE_SECONDS}`,
      'X-Cache': 'HIT'
    };
  }

  return {
    'Cache-Control': `public, max-age=${STATS_CACHE_SECONDS}`,
    'X-Cache': 'MISS'
  };
}

function wantsFresh(c) {
  return c.req.query('fresh') === '1' || c.req.query('fresh') === 'true';
}

async function loadUserStats(username, options = {}) {
  const result = await getUserStats(username, {
    dockerClient,
    forceRefresh: Boolean(options.forceRefresh)
  });
  // Await daily snapshot so history/growth reads see today's sample
  await recordUserSnapshot(redis, result.stats).catch(() => {});
  return result;
}

function parseDays(value, fallback = 30) {
  const days = parseInt(value || String(fallback), 10);
  if (!Number.isFinite(days)) return fallback;
  return Math.max(1, Math.min(days, 90));
}

app.use('/api/*', cors());

// Persist a count of real API calls served (excludes the site's own dashboard
// polling and internal cron). Fire-and-forget via waitUntil, so zero latency.
const CALLS_SKIP = new Set(['/api/portfolio', '/api/views', '/api/health']);
app.use('/api/*', async (c, next) => {
  const cmd = redisClient();
  const path = new URL(c.req.url).pathname;
  if (cmd && !CALLS_SKIP.has(path) && !path.startsWith('/api/internal')) {
    const ep = (path.replace(/^\/api\//, '') || 'root').slice(0, 64);
    const d = new Date().toISOString().slice(0, 10);
    const work = (async () => {
      await cmd(['HINCRBY', K.calls, 'total', '1']);
      await cmd(['HINCRBY', K.calls, 'day:' + d, '1']);
      await cmd(['HEXPIRE', K.calls, String(DAY_TTL), 'FIELDS', '1', 'day:' + d]);
      await cmd(['HINCRBY', K.endpoints, ep, '1']);
      await cmd(['HSET', K.meta, 'lastUpdated', new Date().toISOString()]);
    })().catch(() => {});
    const ctx = c.executionCtx;
    if (ctx && ctx.waitUntil) ctx.waitUntil(work);
  }
  return next();
});

// Docker-Hub data is served by the authenticated Vercel backend; proxy those
// endpoints so Cloudflare egress IPs don't hit Docker Hub's anonymous rate limit.
const DOCKER_BACKEND = 'https://docker-hub-pull-counter.vercel.app';
const NATIVE_PATHS = new Set(['/api/portfolio', '/api/health', '/api/openapi.json', '/api/views']);
app.use('/api/*', async (c, next) => {
  const path = c.req.path;
  if (NATIVE_PATHS.has(path) || path.startsWith('/api/internal')) return next();
  try {
    const u = new URL(c.req.url);
    const r = await fetch(`${DOCKER_BACKEND}${u.pathname}${u.search}`, { headers: { 'User-Agent': 'open-stats-gateway' } });
    const buf = await r.arrayBuffer();
    const h = new Headers(r.headers);
    h.set('Access-Control-Allow-Origin', '*');
    return new Response(buf, { status: r.status, headers: h });
  } catch {
    return next();
  }
});

app.use('/api/*', async (c, next) => {
  const pathName = new URL(c.req.url).pathname;
  // Keep health and public badge total-calls lightly gated but still counted in limiter
  const skipEnforce = pathName === '/api/health';
  const result = await checkRateLimit(c, {
    redis,
    isAuthenticated: Boolean(process.env.DOCKER_USERNAME && process.env.DOCKER_PASSWORD)
  });

  c.set('rateLimitHeaders', result.headers);

  if (!skipEnforce && !result.allowed) {
    const error = new RateLimitError(result.retryAfter || 60);
    return c.json(error.toJSON(), 429, {
      ...result.headers,
      'Retry-After': String(result.retryAfter || 60)
    });
  }

  await next();
});

function rateHeaders(c, extra = {}) {
  return {
    ...(c.get('rateLimitHeaders') || {}),
    ...extra
  };
}

app.get('/api/user/stats', async (c) => {
  const username = c.req.query('username');
  const fieldsParam = c.req.query('fields');
  if (!username) {
    return c.json(new ValidationError('username parameter required', 'username').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('user/stats');

  try {
    const { stats, source } = await loadUserStats(username, {
      forceRefresh: wantsFresh(c)
    });
    return c.json(
      { ...buildUserStatsResponse(stats, fieldsParam), source },
      200,
      rateHeaders(c, cacheHeaders(source))
    );
  } catch (error) {
    console.error('[USER STATS] Error:', error.message);
    return c.json(
      {
        ...errorPayload(error, getUserFacingStatsError(username, error))
      },
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c)
    );
  }
});

app.get('/api/user/top-repos', async (c) => {
  const username = c.req.query('username');
  const limit = parseInt(c.req.query('limit') || '5', 10);
  if (!username) {
    return c.json(new ValidationError('username parameter required', 'username').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('user/top-repos');

  try {
    const { stats, source } = await loadUserStats(username, {
      forceRefresh: wantsFresh(c)
    });
    return c.json(
      {
        success: true,
        username: stats.username,
        limit,
        repositories: getTopRepositories(stats, limit),
        source,
        timestamp: new Date().toISOString()
      },
      200,
      rateHeaders(c, cacheHeaders(source))
    );
  } catch (error) {
    return c.json(
      errorPayload(error, getUserFacingStatsError(username, error)),
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c)
    );
  }
});

app.get('/api/docker-stats', async (c) => {
  const username = c.req.query('username');
  const namespace = c.req.query('namespace');
  const repo = c.req.query('repo');
  const style = c.req.query('style') || 'gradient';

  // Repository card mode
  if (namespace && repo) {
    await trackCall('docker-stats');
    try {
      const data = await fetchDockerHub(`/repositories/${namespace}/${repo}`);
      recordRepoSnapshot(redis, namespace, repo, data).catch(() => {});
      return c.body(createRepoStatsSvgWithStyle(style, data), 200, {
        'Content-Type': 'image/svg+xml; charset=utf-8',
        ...rateHeaders(c, {
          'Cache-Control': `public, max-age=${STATS_CACHE_SECONDS}`
        })
      });
    } catch (error) {
      const message = isNotFoundError(error)
        ? `Repository "${namespace}/${repo}" not found`
        : error.message;
      return c.body(createRepoStatsSvgWithStyle(style, { namespace, repo, error: message }), isNotFoundError(error) ? 404 : 500, {
        'Content-Type': 'image/svg+xml; charset=utf-8',
        ...rateHeaders(c, { 'Cache-Control': 'no-store' })
      });
    }
  }

  if (!username) {
    const svg = createDockerStatsSvgWithStyle(style, {
      username: 'missing-user',
      error: 'username parameter required (or namespace + repo)'
    });
    return c.body(svg, 400, {
      'Content-Type': 'image/svg+xml; charset=utf-8',
      'Cache-Control': 'no-store',
      ...rateHeaders(c)
    });
  }

  await trackCall('docker-stats');

  try {
    const { stats, source } = await loadUserStats(username, {
      forceRefresh: wantsFresh(c)
    });

    return c.body(createDockerStatsSvgWithStyle(style, stats), 200, {
      'Content-Type': 'image/svg+xml; charset=utf-8',
      ...rateHeaders(c, cacheHeaders(source))
    });
  } catch (error) {
    const userMessage = getUserFacingStatsError(username, error);
    const status = isNotFoundError(error) ? 404 : 500;
    return c.body(createDockerStatsSvgWithStyle(style, { username, error: userMessage }), status, {
      'Content-Type': 'image/svg+xml; charset=utf-8',
      'Cache-Control': 'no-store',
      ...rateHeaders(c)
    });
  }
});

app.get('/api/docker-stats/styles', async (c) => {
  const styles = getAvailableStyles();
  return c.json({
    success: true,
    styles,
    default: 'gradient',
    usage: '/api/docker-stats?username=xuxuclassmate&style=gradient',
    repoUsage: '/api/docker-stats?namespace=library&repo=nginx&style=gradient',
    examples: styles.map((style) => ({
      style,
      url: `/api/docker-stats?username=xuxuclassmate&style=${style}`
    }))
  }, 200, rateHeaders(c));
});

app.get('/api/repo/details', async (c) => {
  const namespace = c.req.query('namespace');
  const repo = c.req.query('repo');
  if (!namespace || !repo) {
    return c.json(
      new ValidationError('namespace and repo parameters required').toJSON(),
      400,
      rateHeaders(c)
    );
  }

  await trackCall('repo/details');

  try {
    const data = await fetchDockerHub(`/repositories/${namespace}/${repo}`);
    recordRepoSnapshot(redis, namespace, repo, data).catch(() => {});
    return c.json(
      { success: true, data, timestamp: new Date().toISOString() },
      200,
      rateHeaders(c, { 'Cache-Control': `public, max-age=${STATS_CACHE_SECONDS}` })
    );
  } catch (error) {
    return c.json(
      errorPayload(error),
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c)
    );
  }
});

app.get('/api/repo/tags', async (c) => {
  const namespace = c.req.query('namespace');
  const repo = c.req.query('repo');
  const limit = parseInt(c.req.query('limit') || '100', 10);
  if (!namespace || !repo) {
    return c.json(
      new ValidationError('namespace and repo parameters required').toJSON(),
      400,
      rateHeaders(c)
    );
  }

  await trackCall('repo/tags');

  try {
    const data = await fetchDockerHub(
      `/repositories/${namespace}/${repo}/tags/?page_size=${Math.min(limit, 100)}`
    );
    return c.json(
      {
        success: true,
        namespace,
        repo,
        tags: data.results || [],
        total: data.count,
        timestamp: new Date().toISOString()
      },
      200,
      rateHeaders(c, { 'Cache-Control': `public, max-age=${STATS_CACHE_SECONDS}` })
    );
  } catch (error) {
    return c.json(
      errorPayload(error),
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c)
    );
  }
});

app.get('/api/search', async (c) => {
  const query = c.req.query('q');
  const page = parseInt(c.req.query('page') || '1', 10);
  const pageSize = parseInt(c.req.query('page_size') || '25', 10);
  if (!query) {
    return c.json(new ValidationError('q (search query) parameter required', 'q').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('search');

  try {
    const endpoint = `/search/repositories/?query=${encodeURIComponent(query)}&page=${page}&page_size=${pageSize}`;
    const data = await fetchDockerHub(endpoint, null, 30000);
    return c.json(
      {
        success: true,
        query,
        page,
        pageSize,
        total: data.count || 0,
        results: data.results || [],
        timestamp: new Date().toISOString()
      },
      200,
      rateHeaders(c)
    );
  } catch (error) {
    console.error('[SEARCH] Error:', error.message);
    return c.json(
      {
        ...errorPayload(error),
        hint: 'Search API may be temporarily unavailable. Try again or use specific repository endpoints.'
      },
      500,
      rateHeaders(c)
    );
  }
});

app.get('/api/openapi.json', async (c) => {
  await trackCall('openapi');
  return c.json(generateOpenApiSpec(), 200, {
    'Content-Type': 'application/json; charset=utf-8',
    ...rateHeaders(c, { 'Cache-Control': 'public, max-age=300' })
  });
});

app.get('/api/batch/stats', async (c) => {
  const usernamesParam = c.req.query('usernames');
  if (!usernamesParam) {
    return c.json(new ValidationError('usernames parameter required', 'usernames').toJSON(), 400, rateHeaders(c));
  }

  const usernames = usernamesParam.split(',').map((username) => username.trim()).filter(Boolean);
  if (usernames.length === 0) {
    return c.json(new ValidationError('at least one username required', 'usernames').toJSON(), 400, rateHeaders(c));
  }
  if (usernames.length > 10) {
    return c.json(new ValidationError('maximum 10 usernames allowed', 'usernames').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('batch/stats');

  const results = [];
  const errors = [];
  const forceRefresh = wantsFresh(c);

  await Promise.allSettled(
    usernames.map(async (username) => {
      try {
        const { stats, source } = await loadUserStats(username, { forceRefresh });
        results.push({
          username,
          ...buildUserStatsResponse(stats, null),
          source
        });
      } catch (error) {
        errors.push({ username, error: error.message });
      }
    })
  );

  return c.json(
    {
      success: true,
      total: usernames.length,
      successful: results.length,
      failed: errors.length,
      results,
      errors: errors.length > 0 ? errors : undefined,
      timestamp: new Date().toISOString()
    },
    200,
    rateHeaders(c)
  );
});

app.get('/api/compare', async (c) => {
  const usernamesParam = c.req.query('usernames') || c.req.query('users');
  if (!usernamesParam) {
    return c.json(new ValidationError('usernames parameter required', 'usernames').toJSON(), 400, rateHeaders(c));
  }

  const usernames = usernamesParam.split(',').map((username) => username.trim()).filter(Boolean);
  if (usernames.length < 2) {
    return c.json(new ValidationError('provide at least 2 usernames to compare', 'usernames').toJSON(), 400, rateHeaders(c));
  }
  if (usernames.length > 5) {
    return c.json(new ValidationError('maximum 5 usernames allowed for compare', 'usernames').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('compare');

  const profiles = [];
  const errors = [];

  await Promise.allSettled(
    usernames.map(async (username) => {
      try {
        const { stats, source } = await loadUserStats(username, {
          forceRefresh: wantsFresh(c)
        });
        profiles.push({
          username: stats.username,
          repositoryCount: stats.repositoryCount,
          totalPulls: stats.totalPulls,
          totalStars: stats.totalStars,
          topRepos: getTopRepositories(stats, 3),
          source,
          refreshedAt: stats.refreshedAt
        });
      } catch (error) {
        errors.push({ username, error: error.message });
      }
    })
  );

  profiles.sort((left, right) => right.totalPulls - left.totalPulls);

  return c.json(
    {
      success: true,
      winner: profiles[0]?.username || null,
      rankedBy: 'totalPulls',
      profiles,
      errors: errors.length > 0 ? errors : undefined,
      timestamp: new Date().toISOString()
    },
    200,
    rateHeaders(c)
  );
});

app.get('/api/embed', async (c) => {
  const username = c.req.query('username');
  if (!username) {
    return c.json(new ValidationError('username parameter required', 'username').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('embed');

  try {
    const { stats, source } = await loadUserStats(username, {
      forceRefresh: wantsFresh(c)
    });
    const payload = buildUserBadges(stats, PUBLIC_BASE_URL);
    return c.json(
      {
        success: true,
        ...payload,
        source,
        timestamp: new Date().toISOString()
      },
      200,
      rateHeaders(c, cacheHeaders(source))
    );
  } catch (error) {
    return c.json(
      errorPayload(error, getUserFacingStatsError(username, error)),
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c)
    );
  }
});

app.get('/api/popular/repos', async (c) => {
  const page = Math.max(1, parseInt(c.req.query('page') || '1', 10));
  const pageSize = Math.min(100, Math.max(1, parseInt(c.req.query('page_size') || '25', 10)));
  const namespace = (c.req.query('namespace') || 'library').trim() || 'library';

  await trackCall('popular/repos');

  try {
    // Official/org catalogs support ordering by pull_count
    const data = await fetchDockerHub(
      `/repositories/${encodeURIComponent(namespace)}/?page=${page}&page_size=${pageSize}&ordering=-pull_count`,
      null,
      30000
    );

    return c.json(
      {
        success: true,
        namespace,
        page,
        pageSize,
        total: data.count || 0,
        results: data.results || [],
        ordering: '-pull_count',
        timestamp: new Date().toISOString()
      },
      200,
      rateHeaders(c, { 'Cache-Control': `public, max-age=${STATS_CACHE_SECONDS}` })
    );
  } catch (error) {
    console.error('[POPULAR REPOS] Error:', error.message);
    return c.json(errorPayload(error), 500, rateHeaders(c));
  }
});

app.get('/api/user/history', async (c) => {
  const username = c.req.query('username');
  const days = parseDays(c.req.query('days'), 30);
  if (!username) {
    return c.json(new ValidationError('username parameter required', 'username').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('user/history');

  try {
    // Ensure today's snapshot exists when history is requested
    await loadUserStats(username, { forceRefresh: wantsFresh(c) });
    const history = await getUserHistory(redis, username, days);
    return c.json(
      {
        success: true,
        ...history,
        timestamp: new Date().toISOString()
      },
      200,
      rateHeaders(c, { 'Cache-Control': 'public, max-age=60' })
    );
  } catch (error) {
    return c.json(
      errorPayload(error, getUserFacingStatsError(username, error)),
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c)
    );
  }
});

app.get('/api/user/growth', async (c) => {
  const username = c.req.query('username');
  const days = parseDays(c.req.query('days'), 7);
  if (!username) {
    return c.json(new ValidationError('username parameter required', 'username').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('user/growth');

  try {
    await loadUserStats(username, { forceRefresh: wantsFresh(c) });
    const history = await getUserHistory(redis, username, days);
    return c.json(
      {
        success: true,
        username: history.username,
        days: history.days,
        from: history.from,
        to: history.to,
        insufficientHistory: history.insufficientHistory,
        message: history.insufficientHistory
          ? `Need at least 2 daily snapshots in this window. Collected ${history.sampleCount} so far${history.growth.latestDate ? ` (latest: ${history.growth.latestDate})` : ''}.`
          : undefined,
        growth: history.growth,
        latest: history.points.filter((point) => point.recorded).slice(-1)[0]
          || history.points[history.points.length - 1]
          || null,
        sampleCount: history.sampleCount,
        timestamp: new Date().toISOString()
      },
      200,
      rateHeaders(c, { 'Cache-Control': 'public, max-age=60' })
    );
  } catch (error) {
    return c.json(
      errorPayload(error, getUserFacingStatsError(username, error)),
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c)
    );
  }
});

app.get('/api/repo/history', async (c) => {
  const namespace = c.req.query('namespace');
  const repo = c.req.query('repo');
  const days = parseDays(c.req.query('days'), 30);
  if (!namespace || !repo) {
    return c.json(new ValidationError('namespace and repo parameters required').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('repo/history');

  try {
    const data = await fetchDockerHub(`/repositories/${namespace}/${repo}`);
    await recordRepoSnapshot(redis, namespace, repo, data);
    const history = await getRepoHistory(redis, namespace, repo, days);
    return c.json(
      {
        success: true,
        ...history,
        timestamp: new Date().toISOString()
      },
      200,
      rateHeaders(c, { 'Cache-Control': 'public, max-age=60' })
    );
  } catch (error) {
    return c.json(
      errorPayload(error),
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c)
    );
  }
});

app.get('/api/trend', async (c) => {
  const username = c.req.query('username');
  const namespace = c.req.query('namespace');
  const repo = c.req.query('repo');
  const style = c.req.query('style') || 'gradient';
  const days = parseDays(c.req.query('days'), 30);

  await trackCall('trend');

  try {
    if (namespace && repo) {
      const data = await fetchDockerHub(`/repositories/${namespace}/${repo}`);
      await recordRepoSnapshot(redis, namespace, repo, data);
      const history = await getRepoHistory(redis, namespace, repo, days);
      return c.body(
        createTrendSvgWithStyle(style, {
          username: `${history.namespace}/${history.repo}`,
          ...history
        }),
        200,
        {
          'Content-Type': 'image/svg+xml; charset=utf-8',
          ...rateHeaders(c, { 'Cache-Control': 'public, max-age=120' })
        }
      );
    }

    if (!username) {
      return c.body(
        createTrendSvgWithStyle(style, {
          username: 'missing-user',
          error: 'username parameter required (or namespace + repo)',
          points: [],
          growth: { pulls: 0 }
        }),
        400,
        {
          'Content-Type': 'image/svg+xml; charset=utf-8',
          ...rateHeaders(c, { 'Cache-Control': 'no-store' })
        }
      );
    }

    await loadUserStats(username, { forceRefresh: wantsFresh(c) });
    const history = await getUserHistory(redis, username, days);
    return c.body(createTrendSvgWithStyle(style, history), 200, {
      'Content-Type': 'image/svg+xml; charset=utf-8',
      ...rateHeaders(c, { 'Cache-Control': 'public, max-age=120' })
    });
  } catch (error) {
    const label = username || `${namespace}/${repo}`;
    return c.body(
      createTrendSvgWithStyle(style, {
        username: label,
        error: getUserFacingStatsError(label, error),
        points: [],
        growth: { pulls: 0 }
      }),
      isNotFoundError(error) ? 404 : 500,
      {
        'Content-Type': 'image/svg+xml; charset=utf-8',
        ...rateHeaders(c, { 'Cache-Control': 'no-store' })
      }
    );
  }
});

app.get('/api/stats', async (c) => {
  const stats = await getStats();
  return c.json({
    totalCalls: stats.totalCalls,
    byEndpoint: stats.byEndpoint,
    lastUpdated: stats.lastUpdated
  }, 200, rateHeaders(c, { 'Cache-Control': 'public, max-age=15' }));
});

// Portfolio aggregate: ClawHub + GitHub, computed and cached server-side so the
// website just pulls the latest value instead of every visitor recomputing it.
const PORTFOLIO_CACHE_HEADERS = { 'Cache-Control': 'public, s-maxage=1800, stale-while-revalidate=3600' };
app.get('/api/portfolio', async (c) => {
  const cache = require('./lib/cache');
  const CACHE_KEY = 'portfolio:xuxuclassmate';
  const cached = cache.get(CACHE_KEY);
  if (cached) {
    return c.json(cached, 200, rateHeaders(c, PORTFOLIO_CACHE_HEADERS));
  }

  const USER = 'xuxuclassmate';
  const SKILLS = [
    'skill-publishing', 'free-api-discovery', 'feishu-setup-troubleshooting',
    'ai-testcase-generator', 'stock-monitor-fixed', 'stock-analysis-fixed',
    'test-master-fixed', 'trading-assistant-core', 'whisper-voice-transcription',
    'custom-mail-skill', 'relay'
  ];
  const PLUGINS = ['relay'];
  const jget = (u) =>
    fetch(u, { headers: { 'User-Agent': 'portfolio-stats' } })
      .then((r) => (r.ok ? r.json() : null))
      .catch(() => null);

  const [skRes, pkRes, ghIssues, ghPRs, us, gs, hist] = await Promise.all([
    Promise.all(SKILLS.map((s) => jget(`https://clawhub.ai/api/v1/skills/${s}?owner=${USER}`))),
    Promise.all(PLUGINS.map((n) => jget(`https://clawhub.ai/api/v1/packages/${n}`))),
    jget(`https://api.github.com/search/issues?q=${encodeURIComponent(`author:${USER} type:issue`)}&per_page=1`),
    jget(`https://api.github.com/search/issues?q=${encodeURIComponent(`author:${USER} type:pr`)}&per_page=1`),
    jget(`${DOCKER_BACKEND}/api/user/stats?username=${USER}`),
    jget(`${DOCKER_BACKEND}/api/stats`),
    jget(`${DOCKER_BACKEND}/api/user/history?username=${USER}`)
  ]);

  const rc = redisClient();
  let apiCalls = 0;
  if (rc) apiCalls = Number((await rc(['HGET', K.calls, 'total']))?.result) || 0;

  let clawDownloads = 0;
  let clawSkills = 0;
  let clawPlugins = 0;
  for (const d of skRes) {
    if (d && d.owner && d.owner.handle === USER && d.skill && d.skill.stats && typeof d.skill.stats.downloads === 'number') {
      clawDownloads += d.skill.stats.downloads;
      clawSkills += 1;
    }
  }
  for (const d of pkRes) {
    if (d && d.owner && d.owner.handle === USER && d.package && d.package.stats && typeof d.package.stats.downloads === 'number') {
      clawDownloads += d.package.stats.downloads;
      clawPlugins += 1;
    }
  }

  const pts = (hist && Array.isArray(hist.points)) ? hist.points.map((p) => Number(p.totalPulls)).filter(Number.isFinite) : [];
  const data = {
    pulls: Number(us && us.totalPulls) || 0,
    images: Number(us && us.repositoryCount) || 0,
    apiCalls: apiCalls || Number(gs && gs.totalCalls) || 0,
    monthDelta: pts.length > 1 ? pts[pts.length - 1] - pts[0] : 0,
    history: pts,
    clawDownloads,
    clawSkills,
    clawPlugins,
    ghIssues: Number(ghIssues && ghIssues.total_count) || 0,
    ghPRs: Number(ghPRs && ghPRs.total_count) || 0,
    updatedAt: new Date().toISOString()
  };

  // Only cache a healthy result, so a transient upstream failure self-heals.
  if (clawSkills > 0 || clawPlugins > 0 || data.pulls > 0) {
    cache.set(CACHE_KEY, data, 30 * 60 * 1000);
  }

  return c.json(data, 200, rateHeaders(c, PORTFOLIO_CACHE_HEADERS));
});

async function userBadgeHandler(c, type) {
  const username = c.req.query('username');
  if (!username) {
    return c.json(new ValidationError('username parameter required', 'username').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('badge');

  try {
    const { stats, source } = await loadUserStats(username, {
      forceRefresh: wantsFresh(c)
    });

    const configs = {
      pulls: { label: 'docker pulls', message: formatCompact(stats.totalPulls), color: 'blue' },
      stars: { label: 'docker stars', message: formatCompact(stats.totalStars), color: 'yellow' },
      repos: { label: 'docker repos', message: String(stats.repositoryCount), color: 'informational' }
    };

    return c.json(buildShieldsBadge(configs[type]), 200, rateHeaders(c, cacheHeaders(source)));
  } catch (error) {
    return c.json(
      buildShieldsBadge({ label: `docker ${type}`, message: 'error', color: 'red' }),
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c, { 'Cache-Control': 'no-store' })
    );
  }
}

app.get('/api/badge/pulls', (c) => userBadgeHandler(c, 'pulls'));
app.get('/api/badge/stars', (c) => userBadgeHandler(c, 'stars'));
app.get('/api/badge/repos', (c) => userBadgeHandler(c, 'repos'));

app.get('/api/badge/growth', async (c) => {
  const username = c.req.query('username');
  const days = parseDays(c.req.query('days'), 7);
  if (!username) {
    return c.json(new ValidationError('username parameter required', 'username').toJSON(), 400, rateHeaders(c));
  }

  await trackCall('badge');

  try {
    await loadUserStats(username, { forceRefresh: wantsFresh(c) });
    const history = await getUserHistory(redis, username, days);

    if (history.insufficientHistory) {
      return c.json(
        buildShieldsBadge({
          label: `pulls/${days}d`,
          message: 'collecting',
          color: 'lightgrey'
        }),
        200,
        rateHeaders(c, { 'Cache-Control': 'public, max-age=60' })
      );
    }

    const delta = history.growth.pulls;
    const message = `${delta >= 0 ? '+' : ''}${formatCompact(delta)}`;
    return c.json(
      buildShieldsBadge({
        label: `pulls/${days}d`,
        message,
        color: delta >= 0 ? 'brightgreen' : 'red'
      }),
      200,
      rateHeaders(c, { 'Cache-Control': 'public, max-age=60' })
    );
  } catch (error) {
    return c.json(
      buildShieldsBadge({ label: `pulls/${days}d`, message: 'error', color: 'red' }),
      isNotFoundError(error) ? 404 : 500,
      rateHeaders(c, { 'Cache-Control': 'no-store' })
    );
  }
});

app.get('/api/badge/total-calls', async (c) => {
  const stats = await getStats();
  return c.json(
    buildShieldsBadge({
      label: 'total calls',
      message: String(stats.totalCalls),
      color: 'blue'
    }),
    200,
    rateHeaders(c, { 'Cache-Control': 'public, max-age=30' })
  );
});

app.get('/api/internal/snapshot-history', async (c) => {
  const cronHeader = c.req.header('x-vercel-cron');
  const authHeader = c.req.header('authorization') || '';
  const cronSecret = process.env.CRON_SECRET;
  const authorized = Boolean(cronHeader)
    || (cronSecret && authHeader === `Bearer ${cronSecret}`)
    || c.req.query('secret') === cronSecret;

  if (!authorized) {
    return c.json({ success: false, error: 'Unauthorized', code: 'AUTHENTICATION_ERROR' }, 401, rateHeaders(c));
  }

  await trackCall('internal/snapshot-history');

  const usernames = await listTrackedUsers(redis);
  const refreshed = [];
  const failed = [];

  for (const username of usernames.slice(0, 50)) {
    try {
      await loadUserStats(username, { forceRefresh: true });
      refreshed.push(username);
    } catch (error) {
      failed.push({ username, error: error.message });
    }
  }

  return c.json(
    {
      success: true,
      tracked: usernames.length,
      refreshed: refreshed.length,
      failed: failed.length,
      users: refreshed,
      errors: failed.length > 0 ? failed : undefined,
      timestamp: new Date().toISOString()
    },
    200,
    rateHeaders(c)
  );
});

// ---------------------------------------------------------------------------
// Redis schema (Upstash) -- one `stats:` namespace. Related metrics are grouped
// into HASHes (one key per category, fields inside), with per-field TTL on the
// time-bucketed rows so old data rolls off on its own:
//
//   stats:pageviews         HASH  total | day:YYYY-MM-DD (31d) | month:YYYY-MM (400d)
//   stats:gateway:calls     HASH  total | day:YYYY-MM-DD (31d)
//   stats:gateway:endpoints HASH  <endpoint> -> call count
//   stats:security          HASH  scans30d
//   stats:meta              HASH  schema | lastUpdated
// ---------------------------------------------------------------------------
const K = {
  pageviews: 'stats:pageviews',
  calls: 'stats:gateway:calls',
  endpoints: 'stats:gateway:endpoints',
  security: 'stats:security',
  meta: 'stats:meta'
};
const SCHEMA_VERSION = '3';
const PV_BASELINE = 573771;    // real historical page-view total; applied once if below it
const SCANS_SEED = 400960;     // all-domains last-30d malicious scans (from Cloudflare)
const CALLS_BASELINE = 4996;   // historical API calls, carried over from the Vercel backend
const DAY_TTL = 2678400;       // 31 days
const MONTH_TTL = 34560000;    // 400 days (keeps a full year)
const dayKey = () => new Date().toISOString().slice(0, 10);
const monthKey = () => dayKey().slice(0, 7);

// Raw Upstash REST client. The @upstash/redis SDK's writes are unreliable on
// Workers; a plain command-array POST is not.
function redisClient() {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  return (args) =>
    fetch(url, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(args)
    }).then((r) => (r.ok ? r.json() : null)).catch(() => null);
}

// Give a single hash field its own TTL (Upstash supports hash-field expiry).
const hexpire = (cmd, key, field, ttl) =>
  cmd(['HEXPIRE', key, String(ttl), 'FIELDS', '1', field]);

// One-time, idempotent migration of all legacy flat keys into the stats:* HASHes.
async function migrateStats(cmd) {
  const done = await cmd(['HGET', K.meta, 'schema']);
  if (done && String(done.result) === SCHEMA_VERSION) return;
  const num = async (args) => Number((await cmd(args))?.result);
  const hmax = async (key, field, val) => {
    if (!Number.isFinite(val)) return;
    const cur = Number((await cmd(['HGET', key, field]))?.result) || 0;
    if (val > cur) await cmd(['HSET', key, field, String(val)]);
  };
  // page views (from old pv:* and from the interim stats:pageviews:* string keys)
  await hmax(K.pageviews, 'total', await num(['GET', 'pv:total']));
  await hmax(K.pageviews, 'total', await num(['GET', 'stats:pageviews:total']));
  for (const kk of (await cmd(['KEYS', 'pv:day:*']))?.result || [])
    await hmax(K.pageviews, 'day:' + kk.split(':').pop(), await num(['GET', kk]));
  for (const kk of (await cmd(['KEYS', 'stats:pageviews:daily:*']))?.result || [])
    await hmax(K.pageviews, 'day:' + kk.split(':').pop(), await num(['GET', kk]));
  for (const kk of (await cmd(['KEYS', 'pv:month:*']))?.result || [])
    await hmax(K.pageviews, 'month:' + kk.split(':').pop(), await num(['GET', kk]));
  for (const kk of (await cmd(['KEYS', 'stats:pageviews:monthly:*']))?.result || [])
    await hmax(K.pageviews, 'month:' + kk.split(':').pop(), await num(['GET', kk]));
  // security
  await hmax(K.security, 'scans30d', await num(['GET', 'sec:scans30d']));
  await hmax(K.security, 'scans30d', await num(['GET', 'stats:security:scans:last30d']));
  // gateway calls total (old flat key + vercel baseline)
  await hmax(K.calls, 'total', await num(['GET', 'stats:totalCalls']));
  await hmax(K.calls, 'total', await num(['GET', 'stats:gateway:calls:total']));
  {
    const gs = await fetch(`${DOCKER_BACKEND}/api/stats`, { headers: { 'User-Agent': 'open-stats-gateway' } })
      .then((r) => (r.ok ? r.json() : null)).catch(() => null);
    await hmax(K.calls, 'total', Number(gs && gs.totalCalls) || CALLS_BASELINE);
  }
  // fold the interim byEndpoint hash into the endpoints hash
  const be = (await cmd(['HGETALL', 'stats:gateway:calls:byEndpoint']))?.result;
  if (Array.isArray(be)) for (let i = 0; i < be.length; i += 2) await hmax(K.endpoints, be[i], Number(be[i + 1]) || 0);
  else if (be && typeof be === 'object') for (const [f, v] of Object.entries(be)) await hmax(K.endpoints, f, Number(v) || 0);
  // delete every flat/legacy key -- these patterns never match the HASH keys
  // above (which have no trailing ':')
  for (const pat of ['pv:*', 'sec:*', 'views:*', 'stats:endpoint:*',
                     'stats:pageviews:*', 'stats:security:*', 'stats:gateway:calls:*', 'stats:meta:*'])
    for (const kk of (await cmd(['KEYS', pat]))?.result || []) await cmd(['DEL', kk]);
  for (const kk of ['stats:totalCalls', 'stats:lastUpdated'])
    await cmd(['DEL', kk]);
  await cmd(['HSET', K.meta, 'schema', SCHEMA_VERSION]);
}

app.get('/api/views', async (c) => {
  const noStore = { 'Cache-Control': 'no-store' };
  const empty = { total: 0, today: 0, month: 0, scans30d: 0 };
  const cmd = redisClient();
  if (!cmd) return c.json(empty, 200, rateHeaders(c, noStore));
  try {
    await migrateStats(cmd);
    const d = dayKey();
    const m = monthKey();
    let total = Number((await cmd(['HINCRBY', K.pageviews, 'total', '1']))?.result) || 0;
    if (PV_BASELINE > 0 && total < PV_BASELINE) {
      await cmd(['HSET', K.pageviews, 'total', String(PV_BASELINE)]);
      total = PV_BASELINE;
    }
    const day = Number((await cmd(['HINCRBY', K.pageviews, 'day:' + d, '1']))?.result) || 0;
    await hexpire(cmd, K.pageviews, 'day:' + d, DAY_TTL);
    const month = Number((await cmd(['HINCRBY', K.pageviews, 'month:' + m, '1']))?.result) || 0;
    await hexpire(cmd, K.pageviews, 'month:' + m, MONTH_TTL);
    let scans30d = Number((await cmd(['HGET', K.security, 'scans30d']))?.result) || 0;
    if (!scans30d) { await cmd(['HSET', K.security, 'scans30d', String(SCANS_SEED)]); scans30d = SCANS_SEED; }
    await cmd(['HSET', K.meta, 'lastUpdated', new Date().toISOString()]);
    return c.json({ total, today: day, month, scans30d }, 200, rateHeaders(c, noStore));
  } catch {
    return c.json(empty, 200, rateHeaders(c, noStore));
  }
});

// Diagnostic: dump every Redis key and hash so the schema can be verified.
app.get('/api/internal/stats-dump', async (c) => {
  const cmd = redisClient();
  if (!cmd) return c.json({ error: 'redis-not-configured' }, 200, { 'Cache-Control': 'no-store' });
  const keys = ((await cmd(['KEYS', '*']))?.result || []).sort();
  const dump = {};
  for (const k of keys) {
    const t = (await cmd(['TYPE', k]))?.result;
    if (t === 'hash') {
      const arr = (await cmd(['HGETALL', k]))?.result || [];
      const obj = {};
      if (Array.isArray(arr)) for (let i = 0; i < arr.length; i += 2) obj[arr[i]] = arr[i + 1];
      else Object.assign(obj, arr || {});
      const fields = Object.keys(obj);
      const ttls = fields.length
        ? (await cmd(['HTTL', k, 'FIELDS', String(fields.length), ...fields]))?.result || []
        : [];
      dump[k] = {};
      fields.forEach((f, i) => { dump[k][f] = { v: obj[f], ttl: ttls[i] }; });
    } else {
      dump[k] = (await cmd(['GET', k]))?.result;
    }
  }
  return c.json({ keys, dump }, 200, { 'Cache-Control': 'no-store' });
});

app.get('/api/health', async (c) => {
  return c.json(
    {
      status: 'ok',
      redis: redis ? 'connected' : 'not-configured',
      rateLimit: 'enforced',
      cacheTtlSeconds: STATS_CACHE_SECONDS,
      history: redis ? 'redis' : 'memory',
      timestamp: new Date().toISOString()
    },
    200,
    rateHeaders(c)
  );
});


module.exports = app;
