import time
import unittest
import threading
import os
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import redis
import apigo_discover as d


class TestSDK(unittest.TestCase):
    def test_wsgi_lazy_authenticated_context(self):
        seen = []

        def application(env, start):
            env["HTTP_X_APP_AUTH"] = "authenticated"
            caller = d.newCaller()
            seen.append(caller)
            start("200 OK", [])
            yield b"body"

        environ = {"PATH_INFO": "/", "REQUEST_METHOD": "GET"}
        response = d.wsgi(application)(environ, lambda *args: None)
        self.assertEqual(list(response), [b"body"])
        self.assertEqual(seen[0].request["_wsgi"]["HTTP_X_APP_AUTH"], "authenticated")
        self.assertEqual(seen[0].requestID, environ["HTTP_X_REQUEST_ID"])
        self.assertIsNone(d.newCaller().request)

    def test_contract(self):
        state = {"code": 503, "posts": 0}

        class Failed(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                self.send_response(state["code"])
                self.end_headers()

            def log_message(self, *args):
                pass

        test = self

        class Healthy(BaseHTTPRequestHandler):
            def do_POST(self):
                test.assertEqual(
                    self.rfile.read(int(self.headers.get("Content-Length", 0))),
                    b"payload",
                )
                test.assertTrue(self.headers.get("X-Request-ID"))
                test.assertEqual(self.headers.get_all("X-App-Labels"), ["a", "b"])
                state["posts"] += 1
                self.send_response(201)
                self.end_headers()
                self.wfile.write(b"OK")

            def log_message(self, *args):
                pass

        bad = ThreadingHTTPServer(("127.0.0.1", 0), Failed)
        good = ThreadingHTTPServer(("127.0.0.1", 0), Healthy)
        for server in (bad, good):
            threading.Thread(target=server.serve_forever, daemon=True).start()
        app = f"discover-sdk-python-{os.getpid()}"
        badAddr = f"127.0.0.1:{bad.server_port}"
        goodAddr = f"127.0.0.1:{good.server_port}"
        rd = redis.Redis.from_url("redis://127.0.0.1:6379/15", decode_responses=True)
        d.configure(
            {"app": app, "calls": {app: "1"}, "registry": "redis://127.0.0.1:6379/15"}
        )

        class LB:
            def next(self, call, nodes, request):
                return next((n for n in nodes if n.addr == badAddr), nodes[0])

            def response(self, *args):
                pass

        try:
            d.start(goodAddr)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and (
                not d.isRegistered() or goodAddr not in d.getAppNodes(app)
            ):
                threading.Event().wait(0.01)
            self.assertTrue(rd.hexists(app, goodAddr))
            d.setNode(app, badAddr, 100)
            d.setLoadBalancer(LB())
            caller = d.newCaller({"headers": {"X-App-Labels": ["a", "b"]}})
            self.assertEqual(caller.post(app, "/submit", b"payload").status_code, 201)
            self.assertEqual(state["posts"], 1)
            self.assertEqual(d.getAppNodes(app)[badAddr].failedTimes, 1)
            state["code"] = 200
            self.assertEqual(caller.post(app, "/submit", b"payload").status_code, 200)
            self.assertEqual(d.getAppNodes(app)[badAddr].failedTimes, 0)
            state["code"] = 501
            self.assertEqual(caller.post(app, "/submit", b"payload").status_code, 501)
            self.assertEqual(d.getAppNodes(app)[badAddr].failedTimes, 0)
            self.assertEqual(d.healthHandler()[0], 200)
            self.assertEqual(d.healthHandler()[2], b"OK")
            d.setHealthHandler(lambda request: (503, {}, b"draining"))
            self.assertEqual(d.healthHandler()[0], 503)
            d.stop()
            self.assertFalse(rd.hexists(app, goodAddr))
        finally:
            d.setLoadBalancer(None)
            d.setHealthHandler(None)
            d.stop()
            rd.delete(app, app + "_" + goodAddr)
            rd.close()
            for server in (bad, good):
                server.shutdown()
                server.server_close()


if __name__ == "__main__":
    unittest.main()
