import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import * as discover from "./index.js";
import { createClient } from "redis";

test("health, metadata, POST retries, consecutive failures and Redis lifecycle", async () => {
  let status = 503,
    posts = 0;
  const failed = http.createServer((req, res) => {
    req.resume();
    res.writeHead(status);
    res.end("failed");
  });
  const healthy = http.createServer(
    discover.middleware((req, res) => {
      let data = "";
      req.on("data", (chunk) => (data += chunk));
      req.on("end", () => {
        if (req.method === "POST") {
          assert.equal(data, "payload");
          posts++;
        }
        assert.ok(req.headers["x-request-id"]);
        assert.equal(req.headers["x-user-agent"], "browser");
        assert.equal(req.headers["x-from-app"], app);
        assert.ok(req.headers["x-app-labels"]);
        res.writeHead(201);
        res.end("OK");
      });
    }),
  );
  await Promise.all([
    new Promise((resolve) => failed.listen(0, "127.0.0.1", resolve)),
    new Promise((resolve) => healthy.listen(0, "127.0.0.1", resolve)),
  ]);
  const badAddr = `127.0.0.1:${failed.address().port}`,
    goodAddr = `127.0.0.1:${healthy.address().port}`,
    app = `discover-sdk-node-${process.pid}`;
  const redis = createClient({ url: "redis://127.0.0.1:6379/15" });
  redis.on("error", () => {});
  await redis.connect();
  discover.configure({
    app,
    calls: { [app]: "1" },
    registry: "redis://127.0.0.1:6379/15",
    callRetryTimes: 10,
  });
  try {
    await discover.start(goodAddr);
    assert.ok(await redis.hExists(app, goodAddr));
    assert.ok(await redis.exists(app + "_" + goodAddr));
    discover.setNode(app, badAddr, 100);
    discover.setLoadBalancer({
      next: (_, nodes) => nodes.find((n) => n.addr === badAddr) ?? nodes[0],
      response: () => {},
    });
    const caller = discover.newCaller({
      headers: { "user-agent": "browser", "x-app-labels": ["a", "b"] },
      userAgent: "browser",
    });
    let response = await caller.post(app, "/submit", "payload");
    assert.equal(response.statusCode, 201);
    assert.equal(posts, 1);
    assert.equal(discover.getAppNodes(app)[badAddr].failedTimes, 1);
    status = 200;
    response = await caller.post(app, "/submit", "payload");
    assert.equal(response.statusCode, 200);
    assert.equal(discover.getAppNodes(app)[badAddr].failedTimes, 0);
    status = 501;
    response = await caller.post(app, "/submit", "payload");
    assert.equal(response.statusCode, 501);
    assert.equal(discover.getAppNodes(app)[badAddr].failedTimes, 0);
    const health = await fetch("http://" + goodAddr + discover.healthPath);
    assert.equal(health.status, 200);
    assert.equal(await health.text(), "OK");
    discover.setHealthHandler((req, res) => {
      res.writeHead(503);
      res.end("draining");
    });
    assert.equal(
      (await fetch("http://" + goodAddr + discover.healthPath)).status,
      503,
    );
    await discover.stop();
    assert.equal(Boolean(await redis.hExists(app, goodAddr)), false);
  } finally {
    discover.setHealthHandler(undefined);
    discover.setLoadBalancer(undefined);
    await discover.stop();
    await redis.del(app, app + "_" + goodAddr);
    redis.destroy();
    failed.closeAllConnections();
    healthy.closeAllConnections();
    await Promise.all([
      new Promise((resolve) => failed.close(resolve)),
      new Promise((resolve) => healthy.close(resolve)),
    ]);
  }
});
