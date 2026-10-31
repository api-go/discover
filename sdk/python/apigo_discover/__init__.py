"""Global Discover SDK. Public API uses lowerCamelCase consistently across languages."""

import contextvars
import json
import logging
import random
import os
import threading
import time
import uuid
from dataclasses import dataclass
from urllib.parse import urlsplit
import httpx
import redis

healthPath = "/_discover/health"
relayHeaders = frozenset(
    (
        "x-real-ip x-forwarded-for x-user-id x-device-id x-client-app-name x-client-app-version x-session-id x-request-id x-host x-scheme x-user-agent"
    ).split()
)
config = {
    "registry": "redis://127.0.0.1:6379/15",
    "app": "",
    "weight": 100,
    "calls": {},
    "callRetryTimes": 10,
}
_lock = threading.RLock()
_lifecycle = threading.RLock()
_nodes, _calls, _threads, _http = {}, {}, {}, {}
_stop = threading.Event()
_snapshotLock = threading.RLock()
_pool = None
_addr = ""
_registered = False
_logger = logging.getLogger("apigo.discover")
_route = None
_lb = None
_health = None
_inited = False
_requestContext = contextvars.ContextVar("discoverRequest", default=None)


@dataclass
class NodeInfo:
    addr: str
    weight: int = 100
    usedTimes: int = 0
    failedTimes: int = 0


def init():
    global _inited
    with _lock:
        if _inited:
            return
        for key, env in [
            ("registry", "REGISTRY"),
            ("app", "APP"),
            ("weight", "WEIGHT"),
            ("calls", "CALLS"),
        ]:
            value = os.getenv("DISCOVER_" + env)
            if value is not None:
                config[key] = (
                    int(value)
                    if key == "weight"
                    else json.loads(value) if key == "calls" else value
                )
        _inited = True


def configure(value):
    init()
    config.update(value)


def _parse(value):
    if isinstance(value, dict):
        return {"timeout": 10.0, "httpVersion": 2, "ssl": False, "token": "", **value}
    result = {"timeout": 10.0, "httpVersion": 2, "ssl": False, "token": ""}
    for part in str(value).split(":"):
        if part in ("1", "2"):
            result["httpVersion"] = int(part)
        elif part in ("https", "s"):
            result.update(ssl=True, httpVersion=2)
        elif part in ("http", "h2c"):
            result.update(ssl=False, httpVersion=1 if part == "http" else 2)
        elif part:
            try:
                factor = 0.001 if part.endswith("ms") else 1
                result["timeout"] = (
                    float(
                        part[:-2]
                        if part.endswith("ms")
                        else part[:-1] if part.endswith("s") else part
                    )
                    * factor
                )
            except ValueError:
                result["token"] = part
    return result


def setNode(app, addr, weight):
    with _lock:
        nodes = _nodes.setdefault(app, {})
        if weight <= 0:
            nodes.pop(addr, None)
        elif addr in nodes:
            nodes[addr].weight = weight
        else:
            nodes[addr] = NodeInfo(addr, weight)


def getAppNodes(app):
    with _lock:
        return {
            key: NodeInfo(**vars(node)) for key, node in _nodes.get(app, {}).items()
        }


def _fetch(app):
    with _snapshotLock:
        _fetchSnapshot(app)


def _fetchSnapshot(app):
    if _pool is None:
        return
    values = _pool.eval(_snapshotScript, 1, app)
    live = {values[index]: int(values[index + 1]) for index in range(0, len(values), 2)}
    with _lock:
        for addr in list(_nodes.get(app, {})):
            if addr not in live:
                setNode(app, addr, 0)
        for addr, weight in live.items():
            setNode(app, addr, weight)


def _subscribe(app):
    while not _stop.is_set():
        try:
            with _pool.pubsub() as sub:
                sub.subscribe("CH_" + app)
                while not _stop.is_set():
                    message = sub.get_message(timeout=0.25)
                    if message and message["type"] in ("subscribe", "message"):
                        _fetch(app)
        except (redis.RedisError, ValueError) as error:
            if not _stop.is_set():
                _logger.warning("discover subscription: %s", error)
                _stop.wait(1)


def addExternalApp(app, callConf=""):
    with _lock:
        _calls[app] = _parse(callConf)
        _nodes.setdefault(app, {})
        if _pool is not None and app not in _threads:
            thread = threading.Thread(target=_subscribe, args=(app,), daemon=True)
            _threads[app] = thread
            thread.start()
    return True


def _register():
    global _registered
    app, weight = config["app"], config["weight"]
    _pool.eval(
        _registrationScript,
        2,
        app,
        app + "_" + _addr,
        _addr,
        weight,
        "0" if _registered else "1",
    )
    _registered = True


def isRegistered():
    return _registered


def _heartbeat():
    global _registered
    delay = 1
    while not _stop.is_set():
        try:
            _register()
            delay = 1
        except redis.RedisError as error:
            _registered = False
            _logger.warning("discover heartbeat: %s", error)
            delay = min(4, delay * 2)
        _stop.wait(delay - (random.random() * 0.25 if not _registered else 0))


def _reconcile():
    while not _stop.is_set():
        with _lock:
            apps = list(_calls)
        for app in apps:
            if _stop.is_set():
                return
            try:
                _fetch(app)
            except redis.RedisError as error:
                _logger.warning("discover snapshot: %s", error)
        _stop.wait(2)


def start(addr=""):
    global _pool, _addr, _registered
    with _lifecycle:
        init()
        stop()
        _stop.clear()
        _addr = addr
        _pool = redis.Redis.from_url(
            config["registry"],
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        _registered = False
        if config["app"] and config["weight"] > 0 and addr:
            thread = threading.Thread(target=_heartbeat, daemon=True)
            _threads["@heartbeat"] = thread
            thread.start()
        for app, conf in config["calls"].items():
            addExternalApp(app, conf)
        thread = threading.Thread(target=_reconcile, daemon=True)
        _threads["@reconcile"] = thread
        thread.start()
        return True


def stop():
    global _pool, _addr, _registered
    with _lifecycle:
        _stop.set()
        _registered = False
        for thread in list(_threads.values()):
            thread.join()
        _threads.clear()
        if _pool is not None:
            try:
                if _addr and config["app"]:
                    app = config["app"]
                    _pool.hdel(app, _addr)
                    _pool.delete(app + "_" + _addr)
                    _pool.publish("CH_" + app, f"{_addr} 0")
            except redis.RedisError as error:
                _logger.warning("discover unregister: %s", error)
            finally:
                _pool.close()
                _pool = None
                _addr = ""
        for client in _http.values():
            client.close()
        _http.clear()


def wait():
    _stop.wait()


def isServer():
    return bool(_pool and _addr and config["app"] and config["weight"] > 0)


def isClient():
    return _pool is not None


def setLogger(logger):
    global _logger
    _logger = logger


def setRoute(route):
    global _route
    _route = route


def setLoadBalancer(lb):
    global _lb
    _lb = lb


def setHealthHandler(handler):
    global _health
    _health = handler


def healthHandler(request=None):
    if request is not None and request.get("method", "GET") != "GET":
        return 405, {"Allow": "GET"}, b""
    return (
        _health(request)
        if _health
        else (
            (503, {}, b"Discover registration unavailable")
            if isServer() and not isRegistered()
            else (200, {"Content-Type": "text/plain; charset=utf-8"}, b"OK")
        )
    )


def _headers(request):
    # Normalized RequestContext; adapters expose real peer/TLS metadata.
    request = request or {}
    if "_wsgi" in request:
        request = fromWSGI(request["_wsgi"])
    elif "_asgi" in request:
        request = fromASGI(request["_asgi"])
    headers = []
    raw = request.get("headers", {})
    items = raw.items() if hasattr(raw, "items") else raw
    for name, values in items:
        name = name.lower()
        if name in relayHeaders or name.startswith("x-app-"):
            for value in values if isinstance(values, (list, tuple)) else [values]:
                headers.append((name, str(value)))
    present = {name for name, _ in headers}
    for name, value in [
        ("x-real-ip", request.get("clientIP", "")),
        ("x-host", request.get("host", "")),
        ("x-scheme", request.get("scheme", "http")),
        ("x-user-agent", request.get("userAgent", "")),
        ("x-request-id", uuid.uuid4().hex),
    ]:
        if name not in present and value:
            headers.append((name, value))
    return headers


class Caller:
    def __init__(self, request=None, logger=None):
        self.request = request
        self.logger = logger or _logger
        self.requestID = (
            dict(_headers(request)).get("x-request-id") if request else None
        )

    def do(self, method, app, path, data=None, headers=None, withNode=""):
        context = {
            "method": method,
            "app": app,
            "path": path,
            "data": data,
            "headers": headers or {},
        }
        if _route:
            _route(context, self.request)
        method, app, path, data = (
            context["method"],
            context["app"],
            context["path"],
            context["data"],
        )
        parsed = urlsplit(path)
        if not path.startswith("/") or parsed.scheme or parsed.netloc:
            raise ValueError("path must be local absolute")
        if app not in _calls:
            addExternalApp(app)
        info = _calls[app]
        base = httpx.Headers(_headers(self.request))
        if self.requestID:
            base["x-request-id"] = self.requestID
        for name, value in context["headers"].items():
            if isinstance(value, (list, tuple)):
                base[name] = ", ".join(value)
            else:
                base[name] = str(value)
        if isServer():
            base["X-From-App"] = config["app"]
            base["X-From-Node"] = _addr
        if info["token"]:
            base["Access-Token"] = info["token"]
        with _lock:
            key = (app, info["httpVersion"], info["timeout"])
            client = (
                _http.setdefault(
                    key,
                    httpx.Client(
                        transport=httpx.HTTPTransport(
                            http1=info["ssl"] or info["httpVersion"] == 1,
                            http2=info["httpVersion"] == 2,
                        ),
                        follow_redirects=False,
                    ),
                )
                if key not in _http
                else _http[key]
            )
        if data is not None and not isinstance(
            data, (dict, list, str, bytes, bytearray)
        ):
            raise ValueError("body must be replayable")
        if isinstance(data, (dict, list)):
            data = json.dumps(data).encode()
            if "content-type" not in base:
                base["content-type"] = "application/json"
        excluded = set()
        deadline = time.monotonic() + info["timeout"]
        lastError = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("discover total timeout")
            with _lock:
                allNodes = [
                    n
                    for n in _nodes[app].values()
                    if n.addr not in excluded and (not withNode or n.addr == withNode)
                ]
                ready = [
                    n for n in allNodes if n.failedTimes < config["callRetryTimes"]
                ] or allNodes
                if not ready:
                    raise RuntimeError(f"all nodes failed for {app}") from lastError
                node = (
                    _lb.next(context, ready, self.request)
                    if _lb
                    else min(ready, key=lambda n: n.usedTimes / n.weight)
                )
                node.usedTimes += 1
                excluded.add(node.addr)
            url = ("https" if info["ssl"] else "http") + "://" + node.addr + path
            try:
                response = client.request(
                    method,
                    url,
                    headers=base,
                    timeout=remaining,
                    **(
                        {"json": data}
                        if isinstance(data, (dict, list))
                        else {"content": data}
                    ),
                )
                failed = 502 <= response.status_code <= 504
                lastError = None
            except httpx.TransportError as error:
                response = None
                failed = True
                lastError = error
            with _lock:
                node.failedTimes = node.failedTimes + 1 if failed else 0
            if _lb:
                _lb.response(context, node, lastError, response)
            if not failed:
                response.extensions["discoverNode"] = node.addr
                return response
            if response:
                response.close()

    def get(self, app, path, headers=None):
        return self.do("GET", app, path, headers=headers)

    def post(self, app, path, data=None, headers=None):
        return self.do("POST", app, path, data, headers)

    def put(self, app, path, data=None, headers=None):
        return self.do("PUT", app, path, data, headers)

    def delete(self, app, path, data=None, headers=None):
        return self.do("DELETE", app, path, data, headers)

    def head(self, app, path, headers=None):
        return self.do("HEAD", app, path, headers=headers)

    def doWithNode(self, method, app, withNode, path, data=None, headers=None):
        return self.do(method, app, path, data, headers, withNode)

    def open(self, app, path, headers=None):
        from websockets.sync.client import connect
        from websockets.exceptions import InvalidStatus

        # Handshake selection/retry follows HTTP; established sessions aren't replayed.
        if app not in _calls:
            addExternalApp(app)
        info = _calls[app]
        excluded = set()
        deadline = time.monotonic() + info["timeout"]
        while True:
            with _lock:
                nodes = [n for n in _nodes[app].values() if n.addr not in excluded]
                if not nodes:
                    raise RuntimeError("all websocket nodes failed")
                node = min(nodes, key=lambda n: n.usedTimes / n.weight)
                node.usedTimes += 1
                excluded.add(node.addr)
            hs = httpx.Headers(_headers(self.request))
            hs.update(headers or {})
            if isServer():
                hs["X-From-App"] = config["app"]
                hs["X-From-Node"] = _addr
            if info["token"]:
                hs["Access-Token"] = info["token"]
            try:
                connection = connect(
                    ("wss" if info["ssl"] else "ws") + "://" + node.addr + path,
                    additional_headers=list(hs.multi_items()),
                    open_timeout=max(0.001, deadline - time.monotonic()),
                )
                with _lock:
                    node.failedTimes = 0
                return connection
            except (OSError, TimeoutError, InvalidStatus) as error:
                if (
                    isinstance(error, InvalidStatus)
                    and not 502 <= error.response.status_code <= 504
                ):
                    raise
                with _lock:
                    node.failedTimes += 1
                if time.monotonic() >= deadline:
                    raise TimeoutError("websocket handshake timeout") from error


def newCaller(request=None, logger=None):
    return Caller(request if request is not None else _requestContext.get(), logger)


def asgi(app):
    """One-line ASGI mounting; lifespan registration stays with the server owner."""

    async def wrapped(scope, receive, send):
        if scope["type"] == "http" and scope["path"] == healthPath:
            code, headers, body = healthHandler(
                {"method": scope["method"], "scope": scope}
            )
            await send(
                {
                    "type": "http.response.start",
                    "status": code,
                    "headers": [
                        (k.lower().encode(), v.encode()) for k, v in headers.items()
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body})
        else:
            context = None
            if scope["type"] == "http":
                scope["headers"] = list(scope.get("headers", []))
                if not any(
                    name.lower() == b"x-request-id" for name, _ in scope["headers"]
                ):
                    scope["headers"].append(
                        (b"x-request-id", uuid.uuid4().hex.encode())
                    )
                context = fromASGI(scope)
                context["_asgi"] = scope

            token = _requestContext.set(context) if context else None
            try:
                await app(scope, receive, send)
            finally:
                if token is not None:
                    _requestContext.reset(token)

    return wrapped


def fromASGI(scope):
    return {
        "headers": [(k.decode(), v.decode()) for k, v in scope.get("headers", [])],
        "clientIP": scope.get("client", ("", 0))[0],
        "scheme": scope.get("scheme", "http"),
        "host": dict(scope.get("headers", [])).get(b"host", b"").decode(),
        "userAgent": dict(scope.get("headers", [])).get(b"user-agent", b"").decode(),
    }


def wsgi(app):
    def wrapped(environ, startResponse):
        if environ.get("PATH_INFO") == healthPath:
            code, headers, body = healthHandler(
                {"method": environ["REQUEST_METHOD"], "environ": environ}
            )
            from http import HTTPStatus

            startResponse(f"{code} {HTTPStatus(code).phrase}", list(headers.items()))
            return [body]
        environ.setdefault("HTTP_X_REQUEST_ID", uuid.uuid4().hex)

        def response():
            context = fromWSGI(environ)
            context["_wsgi"] = environ
            token = _requestContext.set(context)
            result = None
            try:
                result = app(environ, startResponse)
                yield from result
            finally:
                if result is not None and hasattr(result, "close"):
                    result.close()
                _requestContext.reset(token)

        return response()

    return wrapped


def fromWSGI(environ):
    return {
        "headers": {
            k[5:].replace("_", "-"): v
            for k, v in environ.items()
            if k.startswith("HTTP_")
        },
        "clientIP": environ.get("REMOTE_ADDR", ""),
        "host": environ.get("HTTP_HOST", ""),
        "scheme": environ.get("wsgi.url_scheme", "http"),
        "userAgent": environ.get("HTTP_USER_AGENT", ""),
    }


_registrationScript = "\nlocal old = redis.call('HGET', KEYS[1], ARGV[1])\nlocal live = redis.call('EXISTS', KEYS[2])\nredis.call('HSET', KEYS[1], ARGV[1], ARGV[2])\nredis.call('SET', KEYS[2], '1', 'EX', 10)\nif old ~= ARGV[2] or live == 0 or ARGV[3] == '1' then\n redis.call('PUBLISH', 'CH_' .. KEYS[1], ARGV[1] .. ' ' .. ARGV[2])\nend\nreturn 1"
_snapshotScript = "local out={}; local nodes=redis.call('HGETALL',KEYS[1]); for i=1,#nodes,2 do if redis.call('EXISTS',KEYS[1]..'_'..nodes[i])==1 then table.insert(out,nodes[i]);table.insert(out,nodes[i+1]);else redis.call('HDEL',KEYS[1],nodes[i]);end;end;return out"
