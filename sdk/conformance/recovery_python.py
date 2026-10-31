import sys, os, time, json, signal, threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../python"))
import redis
import apigo_discover as d

app = os.environ["DISCOVER_TEST_APP"]
target = app + "-target"
d.configure(
    {
        "app": app,
        "calls": {target: "1"},
        "registry": os.environ["DISCOVER_TEST_REGISTRY"],
    }
)
d.config["callRetryTimes"] = 1
retained = None
before = time.monotonic()
d.start("127.0.0.1:31000")
print(json.dumps({"startedMs": (time.monotonic() - before) * 1000}), flush=True)
quit = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: quit.set())
while not quit.wait(0.1):
    if retained is None and d.getAppNodes(target):
        try:
            d.newCaller().get(target, "/")
        except Exception:
            pass
        with redis.Redis.from_url(os.environ["DISCOVER_TEST_REGISTRY"]) as checker:
            retained = checker.hexists(target, "127.0.0.1:31999")
    print(
        json.dumps(
            {
                "globalMembershipRetained": retained,
                "registered": d.isRegistered(),
                "nodes": len(d.getAppNodes(target)),
                "health": d.healthHandler()[0],
            }
        ),
        flush=True,
    )
d.stop()
print(json.dumps({"stopped": True}), flush=True)
