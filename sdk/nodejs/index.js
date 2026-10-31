import { AsyncLocalStorage } from "node:async_hooks";
import { createClient } from "redis";
import http from "node:http";
import https from "node:https";
import http2 from "node:http2";
import { randomUUID, randomInt } from "node:crypto";
import WebSocket from "ws";
const requestStorage = new AsyncLocalStorage();
export const healthPath = "/_discover/health";
export const config = {
  registry: "redis://127.0.0.1:6379/15",
  app: "",
  weight: 100,
  calls: {},
  callRetryTimes: 10,
};
const relay = new Set(
  "x-real-ip x-forwarded-for x-user-id x-device-id x-client-app-name x-client-app-version x-session-id x-request-id x-host x-scheme x-user-agent".split(
    " ",
  ),
);
let heartbeat = Promise.resolve();
let registered = false;
let discovery = Promise.resolve(),
  discoveryTimer,
  wakeDiscovery;
let lifecycle = Promise.resolve();
function serialize(action) {
  const result = lifecycle.then(action, action);
  lifecycle = result.catch(() => {});
  return result;
}
let pool,
  sub,
  addr = "",
  timer,
  inited = false,
  logger = console,
  route,
  lb,
  health,
  running = false;
const nodes = new Map(),
  calls = new Map(),
  subscriptions = new Set();
const agents = {
  http: new http.Agent({ keepAlive: true }),
  https: new https.Agent({ keepAlive: true }),
};
export function init() {
  if (inited) return;
  for (const [key, name] of Object.entries({
    registry: "REGISTRY",
    app: "APP",
    weight: "WEIGHT",
    calls: "CALLS",
  })) {
    const value = process.env["DISCOVER_" + name];
    if (value !== undefined)
      config[key] =
        key === "calls"
          ? JSON.parse(value)
          : key === "weight"
            ? Number(value)
            : value;
  }
  inited = true;
}
export function configure(value) {
  init();
  Object.assign(config, value);
}
function parse(value = "") {
  if (typeof value === "object")
    return { timeout: 10000, httpVersion: 2, ssl: false, token: "", ...value };
  const result = { timeout: 10000, httpVersion: 2, ssl: false, token: "" };
  for (const part of value.split(":")) {
    if (["1", "2"].includes(part)) result.httpVersion = Number(part);
    else if (["s", "https"].includes(part)) {
      result.ssl = true;
      result.httpVersion = 2;
    } else if (["http", "h2c"].includes(part)) {
      result.ssl = false;
      result.httpVersion = part === "http" ? 1 : 2;
    } else if (/^\d+(ms|s)?$/.test(part))
      result.timeout =
        Number(part.replace(/ms|s$/, "")) * (part.endsWith("ms") ? 1 : 1000);
    else if (part) result.token = part;
  }
  return result;
}
export function setNode(app, address, weight) {
  if (!nodes.has(app)) nodes.set(app, new Map());
  const values = nodes.get(app);
  if (weight <= 0) values.delete(address);
  else if (values.has(address)) values.get(address).weight = weight;
  else
    values.set(address, {
      addr: address,
      weight,
      usedTimes: 0,
      failedTimes: 0,
    });
}
export function getAppNodes(app) {
  return Object.fromEntries(
    [...(nodes.get(app)?.entries() ?? [])].map(([key, value]) => [
      key,
      { ...value },
    ]),
  );
}
async function fetchApp(app) {
  if (!pool) return;
  const values = await pool
    .withCommandOptions({ abortSignal: AbortSignal.timeout(1000) })
    .eval(snapshotScript, { keys: [app], arguments: [] });
  const live = new Map();
  for (let index = 0; index < values.length; index += 2)
    live.set(values[index], Number(values[index + 1]));
  for (const address of nodes.get(app)?.keys() ?? [])
    if (!live.has(address)) setNode(app, address, 0);
  for (const [address, weight] of live) setNode(app, address, weight);
}
export async function addExternalApp(app, callConf = "") {
  calls.set(app, parse(callConf));
  if (!nodes.has(app)) nodes.set(app, new Map());
  wakeDiscovery?.();
  return true;
}
export function isRegistered() {
  return registered;
}
async function register() {
  if (!isServer()) return;
  await pool
    .withCommandOptions({ abortSignal: AbortSignal.timeout(1000) })
    .eval(registrationScript, {
      keys: [config.app, config.app + "_" + addr],
      arguments: [addr, String(config.weight), registered ? "0" : "1"],
    });
  registered = true;
}
export function start(address = "") {
  return serialize(() => startRun(address));
}
async function startRun(address) {
  init();
  await stopRun();
  addr = address;
  const options = {
    url: config.registry,
    disableOfflineQueue: true,
    socket: { connectTimeout: 500, reconnectStrategy: false },
  };
  pool = createClient(options);
  sub = pool.duplicate();
  pool.on("error", (error) => logger.error(error));
  sub.on("error", (error) => logger.error(error));
  sub.on("end", () => subscriptions.clear());
  running = true;
  registered = false;
  for (const [app, value] of Object.entries(config.calls))
    await addExternalApp(app, value);
  let delay = 1000;
  const recover = async () => {
    try {
      if (!pool.isOpen) await pool.connect();
      if (!running) return;
      if (isServer()) await register();
      delay = 1000;
    } catch (error) {
      registered = false;
      if (pool.isOpen) pool.destroy();
      logger.error(error);
      delay = Math.min(4000, delay * 2);
    } finally {
      if (running) {
        timer = setTimeout(
          () => {
            heartbeat = recover();
          },
          registered ? delay : delay - randomInt(0, 251),
        );
        timer.unref();
      }
    }
  };
  let reconciling = false,
    reconcileRequested = false;
  const reconcile = async () => {
    if (reconciling) return;
    reconciling = true;
    reconcileRequested = false;
    try {
      if (!pool.isReady || !running) return;
      // Pub/Sub is an optimization. Periodic snapshots recover missed messages.
      try {
        if (!sub.isOpen) {
          subscriptions.clear();
          await sub.connect();
        }
        for (const app of calls.keys()) {
          if (!running) return;
          if (!subscriptions.has(app)) {
            await sub
              .withCommandOptions({ abortSignal: AbortSignal.timeout(1000) })
              .subscribe("CH_" + app, () => {
                wakeDiscovery?.();
              });
            subscriptions.add(app);
          }
        }
      } catch (error) {
        if (sub.isOpen) sub.destroy();
        subscriptions.clear();
        logger.error(error);
      }
      for (const app of calls.keys()) {
        if (!running) return;
        await fetchApp(app);
      }
    } catch (error) {
      logger.error(error);
    } finally {
      reconciling = false;
      if (running) {
        discoveryTimer = setTimeout(
          () => {
            discovery = reconcile();
          },
          reconcileRequested ? 0 : 2000,
        );
        discoveryTimer.unref();
      }
    }
  };
  wakeDiscovery = () => {
    reconcileRequested = true;
    if (!reconciling && running) {
      clearTimeout(discoveryTimer);
      discoveryTimer = setTimeout(() => {
        discovery = reconcile();
      }, 0);
      discoveryTimer.unref();
    }
  };
  wakeDiscovery();
  // Schedule the first network operation after returning startup control.
  timer = setTimeout(() => {
    heartbeat = recover();
  }, 0);
  timer.unref();
  return true;
}
export function stop() {
  return serialize(stopRun);
}
async function stopRun() {
  running = false;
  wakeDiscovery = undefined;
  if (timer) {
    clearTimeout(timer);
    timer = undefined;
  }
  if (discoveryTimer) {
    clearTimeout(discoveryTimer);
    discoveryTimer = undefined;
  }
  if (sub?.isOpen) sub.destroy();
  await Promise.allSettled([heartbeat, discovery]);
  registered = false;
  subscriptions.clear();
  if (pool) {
    try {
      if (addr && config.app && pool.isReady) {
        await pool
          .withCommandOptions({ abortSignal: AbortSignal.timeout(1000) })
          .eval(
            "redis.call('HDEL',KEYS[1],ARGV[1]);redis.call('DEL',KEYS[2]);redis.call('PUBLISH','CH_'..KEYS[1],ARGV[1]..' 0');return 1",
            { keys: [config.app, config.app + "_" + addr], arguments: [addr] },
          );
      }
    } catch (error) {
      logger.error(error);
    } finally {
      if (pool.isOpen) pool.destroy();
      pool = undefined;
    }
  }
  sub = undefined;
  addr = "";
  agents.http.destroy();
  agents.https.destroy();
}
export async function wait() {
  while (running) await new Promise((resolve) => setTimeout(resolve, 100));
  await Promise.allSettled([heartbeat, discovery]);
}
export function isServer() {
  return !!(running && addr && config.app && config.weight > 0);
}
export function isClient() {
  return running;
}
export function setLogger(value) {
  logger = value;
}
export function setRoute(value) {
  route = value;
}
export function setLoadBalancer(value) {
  lb = value;
}
export function setHealthHandler(value) {
  health = value;
}
export function healthHandler(request, response) {
  if (request.method !== "GET") {
    response.writeHead(405, { Allow: "GET" });
    response.end();
    return;
  }
  if (health) return health(request, response);
  const unavailable = isServer() && !registered;
  response.writeHead(unavailable ? 503 : 200, {
    "Content-Type": "text/plain; charset=utf-8",
  });
  response.end(unavailable ? "Discover registration unavailable" : "OK");
}
export function middleware(next) {
  return (request, response) => {
    if (request.url.split("?")[0] === healthPath)
      return healthHandler(request, response);
    const controller = new AbortController();
    response.once("close", () => {
      if (!response.writableFinished)
        controller.abort(new Error("incoming client disconnected"));
    });
    request.headers["x-request-id"] ??= randomUUID().replaceAll("-", "");
    const context = {
      headers: request.headers,
      clientIP: request.socket?.remoteAddress,
      host: request.headers.host,
      userAgent: request.headers["user-agent"],
      scheme: request.socket?.encrypted ? "https" : "http",
      signal: controller.signal,
    };
    return requestStorage.run(context, () => next(request, response));
  };
}
function relayHeaders(request) {
  const result = {};
  for (const [key, value] of Object.entries(request?.headers ?? {})) {
    const name = key.toLowerCase();
    if (relay.has(name) || name.startsWith("x-app-"))
      result[name] = Array.isArray(value) ? [...value] : value;
  }
  result["x-request-id"] ??= randomUUID().replaceAll("-", "");
  if (request) {
    result["x-real-ip"] ??= request.clientIP ?? request.socket?.remoteAddress;
    result["x-user-agent"] ??=
      request.userAgent ?? request.headers?.["user-agent"];
    result["x-host"] ??= request.host ?? request.headers?.host;
    result["x-scheme"] ??=
      request.scheme ?? (request.socket?.encrypted ? "https" : "http");
  }
  for (const key of Object.keys(result))
    if (result[key] === undefined || result[key] === null) delete result[key];
  return result;
}
function send(method, url, data, headers, timeout, version, signal) {
  return new Promise((resolve, reject) => {
    const target = new URL(url);
    let body = data;
    if (
      body !== undefined &&
      body !== null &&
      !Buffer.isBuffer(body) &&
      typeof body !== "string"
    ) {
      body = JSON.stringify(body);
      headers = { "content-type": "application/json", ...headers };
    }
    let session, request;
    if (version === 2) {
      session = http2.connect(target.origin);
      session.on("error", reject);
      request = session.request({
        ":method": method,
        ":path": target.pathname + target.search,
        ...headers,
      });
    } else
      request = (target.protocol === "https:" ? https : http).request(target, {
        method,
        headers,
        agent: agents[target.protocol.slice(0, -1)],
        signal,
      });
    const chunks = [];
    let metadata = {};
    const timeoutId = setTimeout(
      () => request.destroy(new Error("discover timeout")),
      timeout,
    );
    const abort = () =>
      request.destroy(signal.reason ?? new Error("cancelled"));
    if (signal && version === 2) {
      if (signal.aborted) abort();
      else signal.addEventListener("abort", abort, { once: true });
    }
    const cleanup = () => {
      clearTimeout(timeoutId);
      signal?.removeEventListener("abort", abort);
      session?.close();
    };
    request.on("error", (error) => {
      cleanup();
      reject(error);
    });
    const collect = (response) => {
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("error", (error) => {
        cleanup();
        reject(error);
      });
      response.on("end", () => {
        cleanup();
        resolve({
          statusCode: response.statusCode ?? metadata[":status"],
          headers: response.headers ?? metadata,
          body: Buffer.concat(chunks),
          native: response,
        });
      });
    };
    if (version === 2) {
      request.on("response", (headers) => {
        metadata = headers;
      });
      collect(request);
    } else request.on("response", collect);
    request.end(body);
  });
}
export class Caller {
  constructor(request, requestLogger) {
    this.request = request;
    this.requestID = request
      ? relayHeaders(request)["x-request-id"]
      : undefined;
    this.logger = requestLogger ?? logger;
  }
  async do(method, app, path, data, headers = {}, withNode = "") {
    const call = { method, app, path, data, headers };
    if (route) await route(call, this.request);
    ({ method, app, path, data, headers } = call);
    if (!path.startsWith("/") || path.startsWith("//"))
      throw new Error("path must be local absolute");
    if (!calls.has(app)) await addExternalApp(app);
    const info = calls.get(app),
      excluded = new Set(),
      deadline = Date.now() + info.timeout;
    const hs = relayHeaders(this.request);
    if (this.requestID) hs["x-request-id"] = this.requestID;
    for (const [name, value] of Object.entries(headers))
      hs[name.toLowerCase()] = value;
    if (isServer()) {
      hs["x-from-app"] = config.app;
      hs["x-from-node"] = addr;
    }
    if (info.token) hs["access-token"] = info.token;
    if (
      data &&
      (typeof data.pipe === "function" ||
        typeof data[Symbol.asyncIterator] === "function")
    )
      throw new TypeError("body must be replayable");
    if (
      data !== undefined &&
      data !== null &&
      !Buffer.isBuffer(data) &&
      typeof data !== "string"
    ) {
      data = JSON.stringify(data);
      hs["content-type"] ??= "application/json";
    }
    let lastError;
    for (;;) {
      if (this.request?.signal?.aborted) throw this.request.signal.reason;
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error("discover total timeout");
      const all = [...nodes.get(app).values()].filter(
        (n) => !excluded.has(n.addr) && (!withNode || n.addr === withNode),
      );
      let ready = all.filter((n) => n.failedTimes < config.callRetryTimes);
      if (!ready.length) ready = all;
      if (!ready.length)
        throw new Error(`all nodes failed for ${app}`, { cause: lastError });
      const node = lb
        ? lb.next(call, ready, this.request)
        : ready.reduce((a, b) =>
            a.usedTimes / a.weight <= b.usedTimes / b.weight ? a : b,
          );
      excluded.add(node.addr);
      node.usedTimes++;
      let result;
      try {
        result = await send(
          method,
          `${info.ssl ? "https" : "http"}://${node.addr}${path}`,
          data,
          hs,
          remaining,
          info.httpVersion,
          this.request?.signal,
        );
        lastError = undefined;
      } catch (error) {
        if (this.request?.signal?.aborted) throw error;
        lastError = error;
      }
      const failed =
        !!lastError || (result.statusCode >= 502 && result.statusCode <= 504);
      node.failedTimes = failed ? node.failedTimes + 1 : 0;
      if (lb) lb.response(call, node, lastError, result);
      if (!failed) return { ...result, node: node.addr };
    }
  }
  get(app, path, headers) {
    return this.do("GET", app, path, undefined, headers);
  }
  post(app, path, data, headers) {
    return this.do("POST", app, path, data, headers);
  }
  put(app, path, data, headers) {
    return this.do("PUT", app, path, data, headers);
  }
  delete(app, path, data, headers) {
    return this.do("DELETE", app, path, data, headers);
  }
  head(app, path, headers) {
    return this.do("HEAD", app, path, undefined, headers);
  }
  doWithNode(method, app, withNode, path, data, headers) {
    return this.do(method, app, path, data, headers, withNode);
  }
  async open(app, path, headers = {}) {
    if (!calls.has(app)) await addExternalApp(app);
    const info = calls.get(app),
      excluded = new Set();
    for (;;) {
      const ready = [...nodes.get(app).values()].filter(
        (n) => !excluded.has(n.addr),
      );
      if (!ready.length) throw new Error("all websocket nodes failed");
      const node = ready.reduce((a, b) =>
        a.usedTimes / a.weight <= b.usedTimes / b.weight ? a : b,
      );
      excluded.add(node.addr);
      node.usedTimes++;
      const hs = { ...relayHeaders(this.request), ...headers };
      if (isServer()) {
        hs["x-from-app"] = config.app;
        hs["x-from-node"] = addr;
      }
      if (info.token) hs["access-token"] = info.token;
      try {
        const ws = await new Promise((resolve, reject) => {
          const socket = new WebSocket(
            `${info.ssl ? "wss" : "ws"}://${node.addr}${path}`,
            { headers: hs, handshakeTimeout: info.timeout },
          );
          socket.once("open", () => resolve(socket));
          socket.once("error", reject);
        });
        node.failedTimes = 0;
        return ws;
      } catch (error) {
        node.failedTimes++;
      }
    }
  }
}
export function newCaller(request, requestLogger) {
  return new Caller(
    request === undefined ? requestStorage.getStore() : request,
    requestLogger,
  );
}

const registrationScript = `
local old = redis.call('HGET', KEYS[1], ARGV[1])
local live = redis.call('EXISTS', KEYS[2])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('SET', KEYS[2], '1', 'EX', 10)
if old ~= ARGV[2] or live == 0 or ARGV[3] == '1' then
 redis.call('PUBLISH', 'CH_' .. KEYS[1], ARGV[1] .. ' ' .. ARGV[2])
end
return 1`;
const snapshotScript =
  "local out={}; local nodes=redis.call('HGETALL',KEYS[1]); for i=1,#nodes,2 do if redis.call('EXISTS',KEYS[1]..'_'..nodes[i])==1 then table.insert(out,nodes[i]);table.insert(out,nodes[i+1]);else redis.call('HDEL',KEYS[1],nodes[i]);end;end;return out";
