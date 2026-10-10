import http from "node:http";
import * as d from "../nodejs/index.js";
const server = http.createServer(
  d.middleware((req, res) => {
    res.writeHead(404);
    res.end();
  }),
);
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const addr = `127.0.0.1:${server.address().port}`;
d.configure({
  app: process.env.DISCOVER_TEST_PEER,
  registry: "redis://127.0.0.1:6379/15",
  calls: {},
});
await d.start(addr);
console.log("DISCOVER_READY " + addr);
process.on("SIGTERM", async () => {
  await d.stop();
  server.close();
});
