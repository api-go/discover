import json, os, time, threading
import apigo_discover as d
from redis import Redis

app = os.environ["DISCOVER_TEST_PEER"]
addr = os.environ["DISCOVER_TEST_ADDR"]
target = os.environ["DISCOVER_TEST_TARGET"]
d.configure(
    {"app": app, "registry": "redis://127.0.0.1:6379/15", "calls": {target: "1"}}
)
try:
    d.start(addr)
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline and (
        not d.isRegistered() or not d.getAppNodes(target)
    ):
        threading.Event().wait(0.01)
    assert d.isRegistered() and d.getAppNodes(target)
    response = d.newCaller(
        {
            "headers": {"X-App-Labels": ["a", "b"]},
            "clientIP": "192.0.2.4",
            "userAgent": "conformance",
        }
    ).post(target, "/echo", b"payload")
    data = response.json()
    assert data["method"] == "POST" and data["body"] == "payload"
    headers = {k.lower(): v for k, v in data["headers"].items()}
    assert headers["x-from-app"] == [app] and headers["x-app-labels"] == ["a", "b"]
    d.addExternalApp(target, "2")
    assert d.newCaller().post(target, "/echo", b"payload").http_version == "HTTP/2"
    connection = d.newCaller().open(target, "/ws")
    connection.send("hello")
    assert connection.recv() == "hello"
    connection.close()
finally:
    d.stop()
print("PASS Python -> Go HTTP/WebSocket")
