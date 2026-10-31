import * as d from "../nodejs/index.js";
import { createRequire } from "node:module";
const require = createRequire(
  new URL("../nodejs/package.json", import.meta.url),
);
const app = process.env.DISCOVER_TEST_APP,
  target = app + "-target";
d.configure({
  app,
  calls: { [target]: "1" },
  registry: process.env.DISCOVER_TEST_REGISTRY,
});
const before = performance.now();
await d.start("127.0.0.1:31000");
console.log(JSON.stringify({ startedMs: performance.now() - before }));
let retained = null,
  checking = false;
d.config.callRetryTimes = 1;
const tick = setInterval(async () => {
  if (
    retained === null &&
    !checking &&
    Object.keys(d.getAppNodes(target)).length
  ) {
    checking = true;
    try {
      await d.newCaller().get(target, "/");
    } catch {}
    const { createClient } = require("redis");
    const checker = createClient({ url: process.env.DISCOVER_TEST_REGISTRY });
    await checker.connect();
    retained = Boolean(await checker.hExists(target, "127.0.0.1:31999"));
    await checker.close();
  }
  console.log(
    JSON.stringify({
      globalMembershipRetained: retained,
      registered: d.isRegistered(),
      nodes: Object.keys(d.getAppNodes(target)).length,
    }),
  );
}, 100);
process.on("SIGTERM", async () => {
  clearInterval(tick);
  await d.stop();
  console.log(JSON.stringify({ stopped: true }));
  process.exit(0);
});
