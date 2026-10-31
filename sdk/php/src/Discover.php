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
    private const registrationScript = <<<'LUA'

    local old = redis.call('HGET', KEYS[1], ARGV[1])
    local live = redis.call('EXISTS', KEYS[2])
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    redis.call('SET', KEYS[2], '1', 'EX', 10)
    if old ~= ARGV[2] or live == 0 or ARGV[3] == '1' then
     redis.call('PUBLISH', 'CH_' .. KEYS[1], ARGV[1] .. ' ' .. ARGV[2])
    end
    return 1
    LUA;
    private const snapshotScript = "local out={}; local nodes=redis.call('HGETALL',KEYS[1]); for i=1,#nodes,2 do if redis.call('EXISTS',KEYS[1]..'_'..nodes[i])==1 then table.insert(out,nodes[i]);table.insert(out,nodes[i+1]);else redis.call('HDEL',KEYS[1],nodes[i]);end;end;return out";
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
    private static ?int $reconcileTimer = null;
    private static bool $registered = false;
    private static int $generation = 0,
        $workers = 0;
    private static array $subscriptions = [];
    private static array $snapshotVersions = [];
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
        $rd->connect($host, $uri["port"] ?? 6379, 0.5);
        $rd->setOption(\Redis::OPT_READ_TIMEOUT, 0.5);
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
        $version = (self::$snapshotVersions[$app] ?? 0) + 1;
        self::$snapshotVersions[$app] = $version;
        $live = self::redis(function ($rd) use ($app) {
            $values = $rd->eval(self::snapshotScript, [$app], 1);
            if ($values === false) {
                throw new \RuntimeException("Discover snapshot failed");
            }
            $live = [];
            for ($index = 0; $index < count($values); $index += 2) {
                $live[$values[$index]] = (int) $values[$index + 1];
            }
            return $live;
        });
        if (self::$snapshotVersions[$app] !== $version) {
            return;
        }
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
        if (
            self::$running &&
            !self::$fpm &&
            !isset(self::$subscriptions[$app])
        ) {
            self::$subscriptions[$app] = null;
            $generation = self::$generation;
            \Swoole\Timer::after(1, function () use ($app, $generation) {
                if (!self::$running || $generation !== self::$generation) {
                    return;
                }
                self::$workers++;
                \Swoole\Coroutine::create(function () use ($app, $generation) {
                    try {
                        while (
                            self::$running &&
                            $generation === self::$generation
                        ) {
                            try {
                                $rd = self::connection();
                                if (
                                    !self::$running ||
                                    $generation !== self::$generation
                                ) {
                                    try {
                                        $rd->close();
                                    } catch (\Throwable $ignored) {
                                    }
                                    return;
                                }
                                self::$subscriptions[$app] = $rd;
                                $rd->setOption(\Redis::OPT_READ_TIMEOUT, -1);
                                self::fetchApp($app);
                                $rd->subscribe(["CH_" . $app], function (
                                    $rd,
                                    $channel,
                                    $message
                                ) use ($app) {
                                    $parts = explode(" ", $message);
                                    if (
                                        count($parts) === 2 &&
                                        is_numeric($parts[1])
                                    ) {
                                        self::fetchApp($app);
                                    }
                                });
                            } catch (\Throwable $error) {
                                if (self::$running) {
                                    self::log($error);
                                    \Swoole\Coroutine::sleep(0.25);
                                }
                            } finally {
                                if (isset($rd)) {
                                    try {
                                        $rd->close();
                                    } catch (\Throwable $ignored) {
                                    }
                                }
                            }
                        }
                    } finally {
                        self::$workers--;
                    }
                });
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
            $result = $rd->eval(
                self::registrationScript,
                [
                    $app,
                    $app . "_" . $addr,
                    $addr,
                    (string) $weight,
                    self::$registered ? "0" : "1",
                ],
                2,
            );
            if ($result === false) {
                throw new \RuntimeException("Discover registration failed");
            }
        });
        self::$registered = true;
    }
    public static function isRegistered(): bool
    {
        return self::$registered;
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
        self::$registered = false;
        $generation = ++self::$generation;
        foreach (self::$config["calls"] as $app => $conf) {
            self::addExternalApp($app, $conf);
        }
        if (!self::$fpm) {
            $busy = false;
            $retryAt = 0;
            $delay = 1;
            self::$timer = \Swoole\Timer::tick(1000, function () use (
                &$busy,
                &$retryAt,
                &$delay,
                $generation
            ) {
                if (
                    $busy ||
                    !self::$running ||
                    $generation !== self::$generation ||
                    microtime(true) < $retryAt ||
                    !self::isServer()
                ) {
                    return;
                }
                $busy = true;
                self::$workers++;
                \Swoole\Coroutine::create(function () use (
                    &$busy,
                    &$retryAt,
                    &$delay
                ) {
                    try {
                        self::register();
                        $delay = 1;
                    } catch (\Throwable $error) {
                        self::$registered = false;
                        $delay = min(4, $delay * 2);
                        self::log($error);
                    } finally {
                        $retryAt =
                            microtime(true) +
                            $delay -
                            (self::$registered ? 0 : random_int(0, 250) / 1000);
                        $busy = false;
                        self::$workers--;
                    }
                });
            });
            $snapshotBusy = false;
            self::$reconcileTimer = \Swoole\Timer::tick(2000, function () use (
                &$snapshotBusy,
                $generation
            ) {
                if (
                    $snapshotBusy ||
                    !self::$running ||
                    $generation !== self::$generation
                ) {
                    return;
                }
                $snapshotBusy = true;
                self::$workers++;
                \Swoole\Coroutine::create(function () use (&$snapshotBusy) {
                    try {
                        foreach (array_keys(self::$calls) as $app) {
                            if (!self::$running) {
                                break;
                            }
                            try {
                                self::fetchApp($app);
                            } catch (\Throwable $error) {
                                self::log($error);
                            }
                        }
                    } finally {
                        $snapshotBusy = false;
                        self::$workers--;
                    }
                });
            });
        }
        return true;
    }
    public static function stop(): void
    {
        $server = self::isServer();
        self::$running = false;
        self::$generation++;
        self::$registered = false;
        if (self::$reconcileTimer !== null) {
            \Swoole\Timer::clear(self::$reconcileTimer);
            self::$reconcileTimer = null;
        }
        if (self::$timer !== null) {
            \Swoole\Timer::clear(self::$timer);
            self::$timer = null;
        }
        foreach (self::$subscriptions as $rd) {
            if ($rd) {
                try {
                    $rd->close();
                } catch (\Throwable $ignored) {
                }
            }
        }
        self::$subscriptions = [];
        while (self::$workers > 0) {
            \Swoole\Coroutine::sleep(0.01);
        }
        self::$registered = false;
        if ($server) {
            $app = self::$config["app"];
            $addr = self::$addr;
            try {
                self::redis(function ($rd) use ($app, $addr) {
                    $rd->hDel($app, $addr);
                    $rd->del($app . "_" . $addr);
                    $rd->publish("CH_" . $app, "$addr 0");
                });
            } catch (\Throwable $error) {
                self::log($error);
            }
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
            : (self::isServer() && !self::isRegistered()
                ? [503, [], "Discover registration unavailable"]
                : [200, ["Content-Type" => "text/plain; charset=utf-8"], "OK"]);
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
