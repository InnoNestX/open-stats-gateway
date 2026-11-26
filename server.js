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
const CALLS_SKIP = new Set(['/api/portfolio', '/api/views', '/api/health', '/api/track']);
app.use('/api/*', async (c, next) => {
  const cmd = redisClient();
  const path = new URL(c.req.url).pathname;
  if (cmd && !CALLS_SKIP.has(path) && !path.startsWith('/api/internal')) {
    const ep = (path.replace(/^\/api\//, '') || 'root').slice(0, 64);
    const { ym, dd } = parts();
    const work = (async () => {
      await cmd(['INCR', K.callsTotal]);
      await cmd(['HINCRBY', K.callsDaily(ym), dd, '1']);
      await hexpire(cmd, K.callsDaily(ym), dd, DAY_TTL);
      await cmd(['HINCRBY', K.endpoints, ep, '1']);
      await cmd(['SET', K.updated, new Date().toISOString()]);
    })().catch(() => {});
    const ctx = c.executionCtx;
    if (ctx && ctx.waitUntil) ctx.waitUntil(work);
  }
  return next();
});

// Docker-Hub data is served by the authenticated Vercel backend; proxy those
// endpoints so Cloudflare egress IPs don't hit Docker Hub's anonymous rate limit.
const DOCKER_BACKEND = 'https://docker-hub-pull-counter.vercel.app';
const NATIVE_PATHS = new Set(['/api/portfolio', '/api/health', '/api/openapi.json', '/api/views', '/api/track']);
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
  if (rc) apiCalls = Number((await rc(['GET', K.callsTotal]))?.result) || 0;

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
//   stats:pageviews:total              STRING  grand total (all sites)
//   stats:pageviews:<site>:total       STRING  per-site total (www, open, ...)
//   stats:pageviews:<site>:YYYY:MM     HASH    field DD -> views (field TTL 31d)  [site > year > month > days]
//   stats:gateway:calls:total          STRING  API calls served
//   stats:gateway:calls:daily:YYYY-MM  HASH    field DD -> count (field TTL 31d)
//   stats:gateway:endpoints            HASH    <endpoint> -> count
//   stats:security:scans30d            STRING  malicious scans, last 30d
//   stats:meta:schema | :lastUpdated   STRING  schema version / last write
//   stats:analytics:sites              HASH    site -> views
//   stats:analytics:pages[:YYYY-MM]    HASH    "site:path" -> views (monthly bucket TTL)
//   stats:analytics:lang               HASH    lang -> count
//   stats:analytics:referrers          HASH    host -> count
//   stats:analytics:devices            HASH    mobile|tablet|desktop -> count
//   stats:analytics:theme              HASH    light|dark -> count
//   stats:analytics:hours              HASH    0..23 (UTC) -> count
//   stats:analytics:dwell              HASH    totalMs | count  (avg = totalMs/count)
//   stats:analytics:dwellByPage        HASH    "site:path" -> total dwell ms
// ---------------------------------------------------------------------------
const K = {
  pvTotal: 'stats:pageviews:total',                           // grand total (all sites)
  pvSiteTotal: (site) => `stats:pageviews:${site}:total`,     // per-site total
  pvSiteMonth: (site, yr, mo) => `stats:pageviews:${site}:${yr}:${mo}`, // HASH day -> views
  callsTotal: 'stats:gateway:calls:total',
  callsDaily: (ym) => `stats:gateway:calls:daily:${ym}`,
  endpoints: 'stats:gateway:endpoints',
  scans: 'stats:security:scans30d',
  schema: 'stats:meta:schema',
  updated: 'stats:meta:lastUpdated',
  // first-party web analytics, for the future analytics platform
  aSites: 'stats:analytics:sites',
  aPages: 'stats:analytics:pages',
  aPagesMonth: (ym) => `stats:analytics:pages:${ym}`,
  aLang: 'stats:analytics:lang',
  aRef: 'stats:analytics:referrers',
  aDevice: 'stats:analytics:devices',
  aTheme: 'stats:analytics:theme',
  aHours: 'stats:analytics:hours',
  aDwell: 'stats:analytics:dwell',
  aDwellPage: 'stats:analytics:dwellByPage',
  cfHosts: 'stats:analytics:cfHosts'        // HASH hostname -> last-30d requests (from Cloudflare)
};
const SCHEMA_VERSION = '5';
const PV_BASELINE = 573771;    // real historical page-view total; applied once if below it
const SCANS_SEED = 400960;     // all-domains last-30d malicious scans (from Cloudflare)
const CALLS_BASELINE = 4996;   // historical API calls, carried over from the Vercel backend
const DAY_TTL = 2678400;       // 31 days
const MONTH_TTL = 34560000;    // 400 days (keeps a full year)
// date parts for keys/fields: year-month (YYYY-MM), year (YYYY), day-of-month (DD), month (MM)
const parts = () => {
  const iso = new Date().toISOString();
  return { ym: iso.slice(0, 7), y: iso.slice(0, 4), dd: iso.slice(8, 10), mm: iso.slice(5, 7) };
};

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

// Read a hash as a plain object (Upstash REST returns HGETALL as a flat array).
async function hgetallObj(cmd, key) {
  const arr = (await cmd(['HGETALL', key]))?.result;
  const obj = {};
  if (Array.isArray(arr)) for (let i = 0; i < arr.length; i += 2) obj[arr[i]] = arr[i + 1];
  else if (arr && typeof arr === 'object') Object.assign(obj, arr);
  return obj;
}

// One-time, idempotent migration of all legacy flat keys into the stats:* HASHes.
async function migrateStats(cmd) {
  const done = await cmd(['GET', K.schema]);
  if (done && String(done.result) === SCHEMA_VERSION) return;
  // v4 -> v5: move the grand daily page-view hashes under site 'www' (site>year>month>days)
  for (const kk of (await cmd(['KEYS', 'stats:pageviews:daily:*']))?.result || []) {
    const ym = kk.split(':').pop();            // YYYY-MM
    const yr = ym.slice(0, 4), mo = ym.slice(5, 7);
    const days = await hgetallObj(cmd, kk);
    let sum = 0;
    for (const [d, v] of Object.entries(days)) {
      const n = Number(v) || 0;
      if (n <= 0) continue;
      sum += n;
      await cmd(['HSET', K.pvSiteMonth('www', yr, mo), d, String(n)]);
      await hexpire(cmd, K.pvSiteMonth('www', yr, mo), d, DAY_TTL);
    }
    if (sum > 0) {
      const cur = Number((await cmd(['GET', K.pvSiteTotal('www')]))?.result) || 0;
      if (sum > cur) await cmd(['SET', K.pvSiteTotal('www'), String(sum)]);
    }
    await cmd(['DEL', kk]);
  }
  for (const kk of (await cmd(['KEYS', 'stats:pageviews:monthly:*']))?.result || [])
    await cmd(['DEL', kk]);
  if (!(await cmd(['GET', K.scans]))?.result) await cmd(['SET', K.scans, String(SCANS_SEED)]);
  await cmd(['SET', K.schema, SCHEMA_VERSION]);
}

// Read-only: return display numbers. Counting happens in POST /api/track.
app.get('/api/views', async (c) => {
  const noStore = { 'Cache-Control': 'no-store' };
  const empty = { total: 0, today: 0, month: 0, scans30d: 0 };
  const cmd = redisClient();
  if (!cmd) return c.json(empty, 200, rateHeaders(c, noStore));
  try {
    await migrateStats(cmd);
    const { y, dd, mm } = parts();
    const base = Number((await cmd(['GET', K.pvTotal]))?.result) || 0;
    // add subdomains that can't be beaconed (ssh/nas/mail/watch...), measured by Cloudflare
    const cfHosts = await hgetallObj(cmd, K.cfHosts);
    let subSum = 0;
    for (const [host, v] of Object.entries(cfHosts)) {
      const sub = String(host).split('.')[0];
      if (sub !== 'www' && sub !== 'open') subSum += Number(v) || 0;
    }
    const total = (base || PV_BASELINE) + subSum;
    const wwwMonth = await hgetallObj(cmd, K.pvSiteMonth('www', y, mm));
    const today = Number(wwwMonth[dd]) || 0;
    let month = 0;
    for (const v of Object.values(wwwMonth)) month += Number(v) || 0;
    let scans30d = Number((await cmd(['GET', K.scans]))?.result) || 0;
    if (!scans30d) { await cmd(['SET', K.scans, String(SCANS_SEED)]); scans30d = SCANS_SEED; }
    return c.json({ total, today, month, scans30d }, 200, rateHeaders(c, noStore));
  } catch {
    return c.json(empty, 200, rateHeaders(c, noStore));
  }
});

// First-party web-analytics beacon. Body (JSON, or text/plain holding JSON):
//   { event:'view',  site, path, lang, ref, device, theme } -> page view + all dimensions
//   { event:'leave', site, path, dur }                      -> dwell time (ms) on that page
// Owner's own visits are filtered client-side before calling this.
app.post('/api/track', async (c) => {
  const cmd = redisClient();
  if (!cmd) return c.body(null, 204);
  let b = null;
  try { b = JSON.parse(await c.req.text()); } catch { b = null; }
  if (!b || typeof b !== 'object') return c.body(null, 204);
  const { ym, y, dd, mm } = parts();
  const hour = String(new Date().getUTCHours());
  const site = /^[a-z][a-z0-9-]{1,15}$/.test(b.site) ? b.site : 'www';
  const path = (typeof b.path === 'string' && b.path ? b.path : '/').slice(0, 128);
  const sp = `${site}:${path}`.slice(0, 160);
  const lang = /^[a-z]{2}$/.test(b.lang) ? b.lang : 'other';
  const refHost = (() => {
    try { return b.ref ? (new URL(b.ref).hostname || 'direct').slice(0, 80) : 'direct'; }
    catch { return 'direct'; }
  })();
  const device = (b.device === 'mobile' || b.device === 'tablet') ? b.device : 'desktop';
  const theme = b.theme === 'dark' ? 'dark' : 'light';
  const run = (async () => {
    if (b.event === 'view') {
      let total = Number((await cmd(['INCR', K.pvTotal]))?.result) || 0;
      if (total < PV_BASELINE) await cmd(['SET', K.pvTotal, String(PV_BASELINE)]);
      await cmd(['INCR', K.pvSiteTotal(site)]);
      await cmd(['HINCRBY', K.pvSiteMonth(site, y, mm), dd, '1']);
      await hexpire(cmd, K.pvSiteMonth(site, y, mm), dd, DAY_TTL);
      await cmd(['HINCRBY', K.aSites, site, '1']);
      await cmd(['HINCRBY', K.aPages, sp, '1']);
      await cmd(['HINCRBY', K.aPagesMonth(ym), sp, '1']); await hexpire(cmd, K.aPagesMonth(ym), sp, MONTH_TTL);
      await cmd(['HINCRBY', K.aLang, lang, '1']);
      await cmd(['HINCRBY', K.aRef, refHost || 'direct', '1']);
      await cmd(['HINCRBY', K.aDevice, device, '1']);
      await cmd(['HINCRBY', K.aTheme, theme, '1']);
      await cmd(['HINCRBY', K.aHours, hour, '1']);
      await cmd(['SET', K.updated, new Date().toISOString()]);
    } else if (b.event === 'leave') {
      const dur = Math.max(0, Math.min(Math.round(Number(b.dur) || 0), 1800000)); // cap 30 min
      if (dur > 0) {
        await cmd(['HINCRBY', K.aDwell, 'totalMs', String(dur)]);
        await cmd(['HINCRBY', K.aDwell, 'count', '1']);
        await cmd(['HINCRBY', K.aDwellPage, sp, String(dur)]);
      }
    }
  })().catch(() => {});
  const ctx = c.executionCtx;
  if (ctx && ctx.waitUntil) ctx.waitUntil(run); else await run;
  return c.body(null, 204);
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

// Pull per-subdomain request counts + firewall (malicious scan) events from
// Cloudflare analytics into Redis. No-op unless CF_ANALYTICS_TOKEN is set.
// Needs a token with Account Analytics:Read + Zone Analytics:Read + Zone:Read.
async function refreshFromCloudflare(cmd) {
  const token = process.env.CF_ANALYTICS_TOKEN;
  if (!token) return { ok: false, reason: 'no-token' };
  const bearer = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
  const zr = await fetch('https://api.cloudflare.com/client/v4/zones?per_page=50&status=active', { headers: bearer })
    .then((r) => r.json()).catch((e) => ({ errors: [String(e)] }));
  const zones = (zr && zr.result) || [];
  const report = { ok: true, zones: zones.map((z) => z.name), hostCounts: {}, scans: 0, errors: [] };
  if (!zones.length) { report.ok = false; report.errors.push({ zones: (zr && zr.errors) || 'none' }); return report; }
  const since = new Date(Date.now() - 30 * 864e5).toISOString();
  for (const z of zones) {
    const query = 'query($zone:String!,$since:Time!){viewer{zones(filter:{zoneTag:$zone}){'
      + 'hosts:httpRequestsAdaptiveGroups(limit:100,filter:{datetime_geq:$since},orderBy:[count_DESC]){count dimensions{clientRequestHTTPHost}}'
      + ' fw:firewallEventsAdaptiveGroups(limit:1,filter:{datetime_geq:$since}){count}'
      + '}}}';
    const r = await fetch('https://api.cloudflare.com/client/v4/graphql', {
      method: 'POST', headers: bearer, body: JSON.stringify({ query, variables: { zone: z.id, since } })
    }).then((x) => x.json()).catch((e) => ({ errors: [String(e)] }));
    if (r && r.errors && r.errors.length) { report.errors.push({ zone: z.name, errors: r.errors }); continue; }
    const zd = r && r.data && r.data.viewer && r.data.viewer.zones && r.data.viewer.zones[0];
    if (!zd) continue;
    for (const h of (zd.hosts || [])) {
      const host = h.dimensions && h.dimensions.clientRequestHTTPHost;
      if (host) report.hostCounts[host] = (report.hostCounts[host] || 0) + (Number(h.count) || 0);
    }
    for (const f of (zd.fw || [])) report.scans += Number(f.count) || 0;
  }
  const hosts = Object.entries(report.hostCounts);
  if (hosts.length) {
    await cmd(['DEL', K.cfHosts]);
    const args = ['HSET', K.cfHosts];
    for (const [h, n] of hosts) args.push(h.slice(0, 80), String(n));
    await cmd(args);
  }
  if (report.scans > 0) await cmd(['SET', K.scans, String(report.scans)]);
  await cmd(['SET', K.updated, new Date().toISOString()]);
  return report;
}

// Trigger the Cloudflare refresh (also run daily by the cron). Returns a report.
app.get('/api/internal/refresh-cf', async (c) => {
  const cmd = redisClient();
  if (!cmd) return c.json({ ok: false, error: 'redis-not-configured' }, 200, { 'Cache-Control': 'no-store' });
  const report = await refreshFromCloudflare(cmd);
  return c.json(report, 200, { 'Cache-Control': 'no-store' });
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
