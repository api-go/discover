<?php
namespace Apigo\Discover;
// Compatibility helpers for PHP 7.4; header matching remains case insensitive.
function str_starts_with(string $value, string $prefix): bool
{
    return substr($value, 0, strlen($prefix)) === $prefix;
}
function str_ends_with(string $value, string $suffix): bool
{
    return substr($value, -strlen($suffix)) === $suffix;
}

/** Global configuration; each Caller retains only its own incoming context. */
class Discover
{
    public const healthPath = "/_discover/health";
    public static array $config = [
        "registry" => "redis://127.0.0.1:6379/15",
        "app" => "",
        "weight" => 100,
        "calls" => [],
        "callRetryTimes" => 10,
    ];
    public static array $nodes = [],
        $calls = [];
    public static $logger = null,
        $route = null,
        $loadBalancer = null,
        $health = null;
    protected static bool $inited = false,
        $running = false,
        $fpm = false;
    protected static string $addr = "";
    private static ?int $timer = null;
    private static array $subscriptions = [];
    public static function init(): void
    {
        if (self::$inited) {
            return;
        }
        foreach (
            [
                "registry" => "REGISTRY",
                "app" => "APP",
                "weight" => "WEIGHT",
                "calls" => "CALLS",
            ]
            as $key => $name
        ) {
            $value = getenv("DISCOVER_" . $name);
            if ($value !== false) {
                self::$config[$key] =
                    $key === "calls"
                        ? json_decode($value, true, 512, JSON_THROW_ON_ERROR)
                        : ($key === "weight"
                            ? (int) $value
                            : $value);
            }
        }
        self::$inited = true;
    }
    public static function configure(array $config): void
    {
        self::init();
        self::$config = array_replace(self::$config, $config);
    }
    public static function connection(): \Redis
    {
        $uri = parse_url(self::$config["registry"]);
        if (!$uri || !isset($uri["host"])) {
            throw new \InvalidArgumentException(
                "DISCOVER_REGISTRY must be a Redis URI",
            );
        }
        $rd = new \Redis();
        $host = ($uri["scheme"] === "rediss" ? "tls://" : "") . $uri["host"];
        $rd->connect($host, $uri["port"] ?? 6379, 2);
        if (isset($uri["pass"])) {
            $rd->auth(
                isset($uri["user"]) && $uri["user"] !== ""
                    ? [rawurldecode($uri["user"]), rawurldecode($uri["pass"])]
                    : rawurldecode($uri["pass"]),
            );
        }
        $rd->select((int) ltrim($uri["path"] ?? "/0", "/"));
        return $rd;
    }
    public static function redis(callable $operation)
    {
        $rd = self::connection();
        try {
            return $operation($rd);
        } finally {
            $rd->close();
        }
    }
    private static function parse($value): array
    {
        $info = [
            "timeout" => 10.0,
            "httpVersion" => 2,
            "ssl" => false,
            "token" => "",
        ];
        if (is_array($value)) {
            return array_replace($info, $value);
        }
        foreach (explode(":", $value) as $part) {
            if ($part === "1" || $part === "2") {
                $info["httpVersion"] = (int) $part;
            } elseif ($part === "s" || $part === "https") {
                $info["ssl"] = true;
                $info["httpVersion"] = 2;
            } elseif ($part === "http" || $part === "h2c") {
                $info["ssl"] = false;
                $info["httpVersion"] = $part === "http" ? 1 : 2;
            } elseif (preg_match('/^\d+(ms|s)?$/', $part)) {
                $info["timeout"] =
                    (float) $part * (str_ends_with($part, "ms") ? 0.001 : 1);
            } elseif ($part !== "") {
                $info["token"] = $part;
            }
        }
        return $info;
    }
    public static function setNode(string $app, string $addr, int $weight): void
    {
        self::$nodes[$app] ??= [];
        if ($weight <= 0) {
            unset(self::$nodes[$app][$addr]);
        } elseif (isset(self::$nodes[$app][$addr])) {
            self::$nodes[$app][$addr]->weight = $weight;
        } else {
            self::$nodes[$app][$addr] = (object) [
                "addr" => $addr,
                "weight" => $weight,
                "usedTimes" => 0,
                "failedTimes" => 0,
            ];
        }
    }
    public static function getAppNodes(string $app): array
    {
        return array_map(fn($node) => clone $node, self::$nodes[$app] ?? []);
    }
    public static function fetchApp(string $app): void
    {
        if (!self::$running) {
            return;
        }
        $live = self::redis(function ($rd) use ($app) {
            $live = [];
            foreach ($rd->hGetAll($app) as $addr => $weight) {
                if ($rd->exists($app . "_" . $addr)) {
                    $live[$addr] = (int) $weight;
                }
            }
            return $live;
        });
        foreach (array_keys(self::$nodes[$app] ?? []) as $addr) {
            if (!isset($live[$addr])) {
                self::setNode($app, $addr, 0);
            }
        }
        foreach ($live as $addr => $weight) {
            self::setNode($app, $addr, $weight);
        }
    }
    public static function addExternalApp(string $app, $callConf = ""): bool
    {
        self::$calls[$app] = self::parse($callConf);
        self::$nodes[$app] ??= [];
        self::fetchApp($app);
        if (
            self::$running &&
            !self::$fpm &&
            !isset(self::$subscriptions[$app])
        ) {
            self::$subscriptions[$app] = null;
            \Swoole\Coroutine::create(function () use ($app) {
                while (self::$running) {
                    try {
                        $rd = self::connection();
                        self::$subscriptions[$app] = $rd;
                        $rd->setOption(\Redis::OPT_READ_TIMEOUT, -1);
                        self::fetchApp($app);
                        $rd->subscribe(["CH_" . $app], function (
                            $rd,
                            $channel,
                            $message
                        ) use ($app) {
                            $parts = explode(" ", $message);
                            if (count($parts) === 2 && is_numeric($parts[1])) {
                                self::setNode($app, $parts[0], (int) $parts[1]);
                            }
                        });
                    } catch (\Throwable $error) {
                        if (self::$running) {
                            self::log($error);
                            \Swoole\Coroutine::sleep(0.25);
                        }
                    } finally {
                        if (isset($rd)) {
                            $rd->close();
                        }
                    }
                }
            });
        }
        return true;
    }
    private static function register(): void
    {
        if (!self::isServer()) {
            return;
        }
        $app = self::$config["app"];
        $addr = self::$addr;
        $weight = self::$config["weight"];
        self::redis(function ($rd) use ($app, $addr, $weight) {
            if (!$rd->hExists($app, $addr)) {
                $rd->hSet($app, $addr, $weight);
                $rd->setex($app . "_" . $addr, 10, "1");
                $rd->publish("CH_" . $app, "$addr $weight");
            } else {
                $rd->setex($app . "_" . $addr, 10, "1");
            }
        });
    }
    public static function start(string $addr = ""): bool
    {
        self::init();
        self::stop();
        if (!self::$fpm && !class_exists("Swoole\\Coroutine")) {
            throw new \RuntimeException(
                "Resident PHP requires Swoole; use the FPM package for request-scoped applications",
            );
        }
        self::$addr = self::$fpm ? "" : $addr;
        self::$running = true;
        self::redis(fn($rd) => $rd->ping());
        if (!self::$fpm) {
            self::register();
            if (self::isServer()) {
                self::$timer = \Swoole\Timer::tick(1000, function () {
                    \Swoole\Coroutine::create(function () {
                        try {
                            self::register();
                        } catch (\Throwable $error) {
                            self::log($error);
                        }
                    });
                });
            }
        }
        foreach (self::$config["calls"] as $app => $conf) {
            self::addExternalApp($app, $conf);
        }
        return true;
    }
    public static function stop(): void
    {
        $server = self::isServer();
        self::$running = false;
        if (self::$timer !== null) {
            \Swoole\Timer::clear(self::$timer);
            self::$timer = null;
        }
        foreach (self::$subscriptions as $rd) {
            if ($rd) {
                $rd->close();
            }
        }
        self::$subscriptions = [];
        if ($server) {
            $app = self::$config["app"];
            $addr = self::$addr;
            self::redis(function ($rd) use ($app, $addr) {
                $rd->hDel($app, $addr);
                $rd->del($app . "_" . $addr);
                $rd->publish("CH_" . $app, "$addr 0");
            });
        }
        self::$addr = "";
    }
    public static function wait(): void
    {
        if (self::$fpm) {
            throw new \LogicException("FPM does not own a service lifecycle");
        }
        while (self::$running) {
            \Swoole\Coroutine::sleep(0.25);
        }
    }
    public static function isServer(): bool
    {
        return self::$running &&
            !self::$fpm &&
            self::$addr !== "" &&
            self::$config["app"] !== "" &&
            self::$config["weight"] > 0;
    }
    public static function isClient(): bool
    {
        return self::$running;
    }
    public static function isFpm(): bool
    {
        return self::$fpm;
    }
    public static function source(): array
    {
        return [
            "X-From-App" => self::$config["app"],
            "X-From-Node" => self::$addr,
        ];
    }
    public static function setLogger($logger): void
    {
        self::$logger = $logger;
    }
    public static function log(\Throwable $error): void
    {
        if (self::$logger) {
            (self::$logger)($error);
        }
    }
    public static function setRoute($route): void
    {
        self::$route = $route;
    }
    public static function setLoadBalancer($lb): void
    {
        self::$loadBalancer = $lb;
    }
    public static function setHealthHandler($handler): void
    {
        self::$health = $handler;
    }
    public static function healthHandler(array $request = []): array
    {
        if (($request["method"] ?? "GET") !== "GET") {
            return [405, ["Allow" => "GET"], ""];
        }
        return self::$health
            ? (self::$health)($request)
            : [200, ["Content-Type" => "text/plain; charset=utf-8"], "OK"];
    }
    public static function middleware(callable $next): callable
    {
        return fn(
            array $request
        ) => ($request["path"] ?? "") === self::healthPath ? self::healthHandler($request) : $next($request);
    }
    public static function swooleMiddleware(callable $next): callable
    {
        return function ($request, $response) use ($next) {
            if (($request->server["request_uri"] ?? "") !== self::healthPath) {
                $context = \Swoole\Coroutine::getContext();
                $request->header["x-request-id"] =
                    $request->header["x-request-id"] ??
                    bin2hex(random_bytes(16));
                $context["discoverRequest"] = [
                    "swooleRequest" => $request,
                    "headers" => $request->header ?? [],
                    "clientIP" => $request->server["remote_addr"] ?? "",
                    "host" => $request->header["host"] ?? "",
                    "userAgent" => $request->header["user-agent"] ?? "",
                    "scheme" => "http",
                ];
                try {
                    return $next($request, $response);
                } finally {
                    unset($context["discoverRequest"]);
                }
            }
            [$code, $headers, $body] = self::healthHandler([
                "method" => strtoupper($request->server["request_method"]),
            ]);
            $response->status($code);
            foreach ($headers as $k => $v) {
                $response->header($k, $v);
            }
            $response->end($body);
        };
    }
    public static function newCaller($request = null, $logger = null): Caller
    {
        if ($request === null) {
            if (self::$fpm) {
                $request = self::fromGlobals();
            } elseif (
                class_exists("Swoole\\Coroutine") &&
                \Swoole\Coroutine::getCid() >= 0
            ) {
                $request =
                    \Swoole\Coroutine::getContext()["discoverRequest"] ?? null;
            }
        }
        return new Caller($request, $logger);
    }
    public static function fromGlobals(): array
    {
        $_SERVER["HTTP_X_REQUEST_ID"] =
            $_SERVER["HTTP_X_REQUEST_ID"] ?? bin2hex(random_bytes(16));
        $headers = [];
        foreach ($_SERVER as $key => $value) {
            if (str_starts_with($key, "HTTP_")) {
                $headers[str_replace("_", "-", substr($key, 5))] = $value;
            }
        }
        return [
            "headers" => $headers,
            "clientIP" => $_SERVER["REMOTE_ADDR"] ?? "",
            "host" => $_SERVER["HTTP_HOST"] ?? "",
            "scheme" => empty($_SERVER["HTTPS"]) ? "http" : "https",
            "userAgent" => $_SERVER["HTTP_USER_AGENT"] ?? "",
        ];
    }
}
