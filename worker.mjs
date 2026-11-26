// Cloudflare Worker entry for the ported Hono app (server.js, CommonJS).
// server.js reads process.env at module scope (Upstash Redis client), which on
// Workers is empty at isolate init. So we inject env -> process.env and then
// dynamically import server.js on the first request, once env is populated.
let appPromise;

async function getApp(env) {
  if (!appPromise) {
    for (const k in env) {
      if (typeof env[k] === "string") process.env[k] = env[k];
    }
    appPromise = import("./server.js").then((m) => m.default || m);
  }
  return appPromise;
}

export default {
  async fetch(request, env, ctx) {
    const app = await getApp(env);
    return app.fetch(request, env, ctx);
  },
  async scheduled(event, env, ctx) {
    const app = await getApp(env);
    ctx.waitUntil(
      app.fetch(new Request("https://open.xuxuclassmate.com/api/internal/snapshot-history"), env, ctx),
    );
    ctx.waitUntil(
      app.fetch(new Request("https://open.xuxuclassmate.com/api/internal/refresh-cf"), env, ctx),
    );
  },
};
