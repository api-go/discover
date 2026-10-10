import assert from "node:assert/strict";
import * as d from "../nodejs/index.js";
const app = process.env.DISCOVER_TEST_PEER,
  addr = process.env.DISCOVER_TEST_ADDR,
  target = process.env.DISCOVER_TEST_TARGET;
d.configure({
  app,
  registry: "redis://127.0.0.1:6379/15",
  calls: { [target]: "1" },
});
try {
  await d.start(addr);
  const response = await d
    .newCaller({
      headers: { "X-App-Labels": ["a", "b"] },
      clientIP: "192.0.2.4",
      userAgent: "conformance",
    })
    .post(target, "/echo", "payload");
  const data = JSON.parse(response.body);
  assert.equal(data.method, "POST");
  assert.equal(data.body, "payload");
  const headers = Object.fromEntries(
    Object.entries(data.headers).map(([k, v]) => [k.toLowerCase(), v]),
  );
  assert.deepEqual(headers["x-from-app"], [app]);
  assert.equal(headers["x-app-labels"].join(", "), "a, b");
  await d.addExternalApp(target, "2");
  assert.equal((await d.newCaller().get(target, "/echo")).statusCode, 200);
  const ws = await d.newCaller().open(target, "/ws");
  const result = new Promise((resolve) =>
    ws.once("message", (data) => resolve(data.toString())),
  );
  ws.send("hello");
  assert.equal(await result, "hello");
  ws.close();
} finally {
  await d.stop();
}
console.log("PASS Node.js -> Go HTTP/WebSocket");
