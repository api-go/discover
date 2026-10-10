import os, signal, threading
from wsgiref.simple_server import make_server
import apigo_discover as d

app = d.wsgi(lambda env, start: (start("404 Not Found", []), [b""])[1])
server = make_server("127.0.0.1", 0, app)
addr = f"127.0.0.1:{server.server_port}"
d.configure(
    {
        "app": os.environ["DISCOVER_TEST_PEER"],
        "registry": "redis://127.0.0.1:6379/15",
        "calls": {},
    }
)
d.start(addr)
threading.Thread(target=server.serve_forever, daemon=True).start()
print("DISCOVER_READY " + addr, flush=True)


def stop(signum, frame):
    raise SystemExit(0)


signal.signal(signal.SIGTERM, stop)
try:
    signal.pause()
finally:
    d.stop()
    server.shutdown()
    server.server_close()
