<?php
namespace Apigo\Discover;
class Caller
{
    private array $request;
    private $logger;
    public function __construct($request = null, $logger = null)
    {
        if (is_object($request) && method_exists($request, "getHeaders")) {
            $uri = $request->getUri();
            $params = method_exists($request, "getServerParams")
                ? $request->getServerParams()
                : [];
            $request = [
                "headers" => $request->getHeaders(),
                "clientIP" => $params["REMOTE_ADDR"] ?? "",
                "host" => $uri->getHost(),
                "scheme" => $uri->getScheme(),
                "userAgent" => $request->getHeaderLine("User-Agent"),
            ];
        }
        if (is_object($request)) {
            throw new \InvalidArgumentException(
                "Expected PSR-7 request or normalized context",
            );
        }
        $this->request = $request ?? [];
        $this->logger = $logger;
    }
    private function headers(array $explicit): array
    {
        $allowed = explode(
            " ",
            "x-real-ip x-forwarded-for x-user-id x-device-id x-client-app-name x-client-app-version x-session-id x-request-id x-host x-scheme x-user-agent",
        );
        $headers = [];
        $incomingHeaders = isset($this->request["swooleRequest"])
            ? $this->request["swooleRequest"]->header
            : $this->request["headers"] ?? [];
        foreach ($incomingHeaders as $key => $value) {
            $name = strtolower($key);
            if (
                in_array($name, $allowed, true) ||
                str_starts_with($name, "x-app-")
            ) {
                $headers[$name] = (array) $value;
            }
        }
        foreach (
            [
                "x-real-ip" => $this->request["clientIP"] ?? "",
                "x-host" => $this->request["host"] ?? "",
                "x-scheme" => $this->request["scheme"] ?? "http",
                "x-user-agent" => $this->request["userAgent"] ?? "",
                "x-request-id" => bin2hex(random_bytes(16)),
            ]
            as $key => $value
        ) {
            if (!isset($headers[$key]) && $value !== "") {
                $headers[$key] = [$value];
            }
        }
        foreach ($explicit as $key => $value) {
            $headers[strtolower($key)] = (array) $value;
        }
        if (Discover::isServer()) {
            foreach (Discover::source() as $key => $value) {
                $headers[strtolower($key)] = [$value];
            }
        }
        return $headers;
    }
    public function do(
        string $method,
        string $app,
        string $path,
        $data = null,
        array $headers = [],
        string $withNode = ""
    ): array {
        $call = [
            "method" => $method,
            "app" => $app,
            "path" => $path,
            "data" => $data,
            "headers" => $headers,
        ];
        if (Discover::$route) {
            $call = (Discover::$route)($call, $this->request) ?? $call;
        }
        extract($call, EXTR_OVERWRITE);
        if (!str_starts_with($path, "/") || str_starts_with($path, "//")) {
            throw new \InvalidArgumentException("path must be local absolute");
        }
        if (!isset(Discover::$calls[$app])) {
            Discover::addExternalApp($app);
        }
        if (Discover::isFpm()) {
            Discover::fetchApp($app);
        }
        $info = Discover::$calls[$app];
        $hs = $this->headers($headers);
        if ($info["token"] !== "") {
            $hs["access-token"] = [$info["token"]];
        }
        if (is_resource($data)) {
            throw new \InvalidArgumentException("Body must be replayable");
        }
        if (is_array($data) || is_object($data)) {
            $data = json_encode($data, JSON_THROW_ON_ERROR);
            $hs["content-type"] ??= ["application/json"];
        }
        $flat = [];
        foreach ($hs as $name => $values) {
            foreach ($values as $value) {
                $flat[] = "$name: $value";
            }
        }
        $excluded = [];
        $deadline = microtime(true) + $info["timeout"];
        while (true) {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                throw new \RuntimeException("discover total timeout");
            }
            $all = array_filter(
                Discover::$nodes[$app],
                fn($n) => !isset($excluded[$n->addr]) &&
                    ($withNode === "" || $withNode === $n->addr),
            );
            $ready = array_filter(
                $all,
                fn($n) => $n->failedTimes < Discover::$config["callRetryTimes"],
            );
            if (!$ready) {
                $ready = $all;
            }
            if (!$ready) {
                throw new \RuntimeException("all nodes failed for $app");
            }
            uasort(
                $ready,
                fn($a, $b) => $a->usedTimes / $a->weight <=>
                    $b->usedTimes / $b->weight,
            );
            $node = Discover::$loadBalancer
                ? Discover::$loadBalancer->next(
                    $call,
                    array_values($ready),
                    $this->request,
                )
                : reset($ready);
            $excluded[$node->addr] = true;
            $node->usedTimes++;
            if (!Discover::isFpm() && \Swoole\Coroutine::getCid() >= 0) {
                $parsed = parse_url("http://" . $node->addr);
                $nativeHeaders = [];
                foreach ($hs as $name => $values) {
                    $nativeHeaders[$name] = implode(", ", $values);
                }
                if ($info["httpVersion"] === 2) {
                    $client = new \Swoole\Coroutine\Http2\Client(
                        $parsed["host"],
                        $parsed["port"] ?? 80,
                        $info["ssl"],
                    );
                    $client->set(["timeout" => $remaining]);
                    $request = new \Swoole\Http2\Request();
                    $request->method = $method;
                    $request->path = $path;
                    $request->headers = $nativeHeaders;
                    $request->data = $data ?? "";
                    $ok =
                        $client->connect() && $client->send($request) !== false;
                    $response = $ok ? $client->recv($remaining) : false;
                    $body = $response ? $response->data : false;
                    $code = $response ? $response->statusCode : 0;
                    $error = $client->errMsg;
                    $rawHeaders = $response ? $response->headers : [];
                } else {
                    $client = new \Swoole\Coroutine\Http\Client(
                        $parsed["host"],
                        $parsed["port"] ?? 80,
                        $info["ssl"],
                    );
                    $client->set(["timeout" => $remaining]);
                    $client->setMethod($method);
                    $client->setHeaders($nativeHeaders);
                    if ($data !== null) {
                        $client->setData($data);
                    }
                    $ok = $client->execute($path);
                    $body = $ok ? $client->body : false;
                    $error = $client->errMsg;
                    $code = $client->statusCode;
                    $rawHeaders = $client->headers ?? [];
                }
                $responseHeaders = [];
                foreach ($rawHeaders as $key => $value) {
                    $responseHeaders[$key] = (array) $value;
                }
                $client->close();
            } else {
                $ch = curl_init(
                    ($info["ssl"] ? "https" : "http") .
                        "://" .
                        $node->addr .
                        $path,
                );
                $responseHeaders = [];
                curl_setopt_array($ch, [
                    CURLOPT_CUSTOMREQUEST => $method,
                    CURLOPT_HTTPHEADER => $flat,
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_FOLLOWLOCATION => false,
                    CURLOPT_TIMEOUT_MS => max(1, (int) ($remaining * 1000)),
                    CURLOPT_HTTP_VERSION =>
                        $info["httpVersion"] === 2
                            ? ($info["ssl"]
                                ? CURL_HTTP_VERSION_2TLS
                                : CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE)
                            : CURL_HTTP_VERSION_1_1,
                    CURLOPT_HEADERFUNCTION => function ($ch, $line) use (
                        &$responseHeaders
                    ) {
                        $parts = explode(":", $line, 2);
                        if (count($parts) === 2) {
                            $responseHeaders[
                                strtolower(trim($parts[0]))
                            ][] = trim($parts[1]);
                        }
                        return strlen($line);
                    },
                ]);
                if ($method === "HEAD") {
                    curl_setopt($ch, CURLOPT_NOBODY, true);
                }
                if ($data !== null) {
                    curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
                }
                $body = curl_exec($ch);
                $error = curl_error($ch);
                $code = curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
                curl_close($ch);
            }
            $failed = $body === false || ($code >= 502 && $code <= 504);
            $node->failedTimes = $failed ? $node->failedTimes + 1 : 0;
            $result = [
                "statusCode" => $code,
                "headers" => $responseHeaders,
                "body" => $body,
                "node" => $node->addr,
            ];
            if (Discover::$loadBalancer) {
                Discover::$loadBalancer->response(
                    $call,
                    $node,
                    $error,
                    $result,
                );
            }
            if (!$failed) {
                return $result;
            }
        }
    }
    public function open(
        string $app,
        string $path,
        array $headers = []
    ): \Swoole\Coroutine\Http\Client {
        if (Discover::isFpm()) {
            throw new \LogicException(
                "FPM does not own persistent WebSocket sessions",
            );
        }
        if (!str_starts_with($path, "/") || str_starts_with($path, "//")) {
            throw new \InvalidArgumentException("path must be local absolute");
        }
        if (!isset(Discover::$calls[$app])) {
            Discover::addExternalApp($app);
        }
        $info = Discover::$calls[$app];
        $hs = $this->headers($headers);
        if ($info["token"] !== "") {
            $hs["access-token"] = [$info["token"]];
        }
        $native = [];
        foreach ($hs as $key => $values) {
            $native[$key] = implode(", ", $values);
        }
        $excluded = [];
        $deadline = microtime(true) + $info["timeout"];
        while (true) {
            $ready = array_filter(
                Discover::$nodes[$app],
                fn($node) => !isset($excluded[$node->addr]),
            );
            if (!$ready) {
                throw new \RuntimeException("all websocket nodes failed");
            }
            uasort(
                $ready,
                fn($a, $b) => $a->usedTimes / $a->weight <=>
                    $b->usedTimes / $b->weight,
            );
            $node = reset($ready);
            $excluded[$node->addr] = true;
            $node->usedTimes++;
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                throw new \RuntimeException("websocket handshake timeout");
            }
            $uri = parse_url("http://" . $node->addr);
            $client = new \Swoole\Coroutine\Http\Client(
                $uri["host"],
                $uri["port"] ?? 80,
                $info["ssl"],
            );
            $client->set(["timeout" => $remaining]);
            $client->setHeaders($native);
            if ($client->upgrade($path)) {
                $node->failedTimes = 0;
                return $client;
            }
            $code = $client->statusCode;
            $client->close();
            if ($code > 0 && ($code < 502 || $code > 504)) {
                throw new \RuntimeException("websocket rejected: " . $code);
            }
            $node->failedTimes++;
        }
    }
    public function get(string $app, string $path, array $headers = []): array
    {
        return $this->do("GET", $app, $path, null, $headers);
    }
    public function post(
        string $app,
        string $path,
        $data = null,
        array $headers = []
    ): array {
        return $this->do("POST", $app, $path, $data, $headers);
    }
    public function put(
        string $app,
        string $path,
        $data = null,
        array $headers = []
    ): array {
        return $this->do("PUT", $app, $path, $data, $headers);
    }
    public function delete(
        string $app,
        string $path,
        $data = null,
        array $headers = []
    ): array {
        return $this->do("DELETE", $app, $path, $data, $headers);
    }
    public function head(string $app, string $path, array $headers = []): array
    {
        return $this->do("HEAD", $app, $path, null, $headers);
    }
    public function doWithNode(
        string $method,
        string $app,
        string $withNode,
        string $path,
        $data = null,
        array $headers = []
    ): array {
        return $this->do($method, $app, $path, $data, $headers, $withNode);
    }
}
