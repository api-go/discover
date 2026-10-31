<?php
require __DIR__ . "/../php/src/Discover.php";
require __DIR__ . "/../php/src/Caller.php";
require __DIR__ . "/../php-fpm/src/Discover.php";
use Apigo\Discover\Discover;
$app = getenv("DISCOVER_TEST_PEER");
$addr = getenv("DISCOVER_TEST_ADDR");
$target = getenv("DISCOVER_TEST_TARGET");
$fpm = getenv("DISCOVER_TEST_FPM") === "1";
$class = $fpm ? Apigo\Discover\Fpm\Discover::class : Discover::class;
$class::configure([
    "app" => $app,
    "registry" => "redis://127.0.0.1:6379/15",
    "calls" => [$target => "1"],
]);
$run = function () use ($class, $app, $addr, $target, $fpm) {
    try {
        $class::start($addr);
        if (!$fpm) {
            $deadline = microtime(true) + 8;
            while (
                microtime(true) < $deadline &&
                (!$class::isRegistered() || !$class::getAppNodes($target))
            ) {
                \Swoole\Coroutine::sleep(0.01);
            }
        }
        $registered = Discover::redis(fn($rd) => $rd->hExists($app, $addr));
        if ($registered === $fpm) {
            throw new RuntimeException("wrong lifecycle ownership");
        }
        $result = $class
            ::newCaller([
                "headers" => ["X-App-Labels" => ["a", "b"]],
                "clientIP" => "192.0.2.4",
                "userAgent" => "conformance",
            ])
            ->post($target, "/echo", "payload");
        $data = json_decode($result["body"], true, 512, JSON_THROW_ON_ERROR);
        if ($data["method"] !== "POST" || $data["body"] !== "payload") {
            throw new RuntimeException("body mismatch");
        }
        $headers = array_change_key_case($data["headers"]);
        if (implode(", ", $headers["x-app-labels"]) !== "a, b") {
            throw new RuntimeException("multi-value headers mismatch");
        }
        if (!$fpm && $headers["x-from-app"] !== [$app]) {
            throw new RuntimeException("source mismatch");
        }
        $class::addExternalApp($target, "2");
        if ($class::newCaller()->get($target, "/echo")["statusCode"] !== 200) {
            throw new RuntimeException("H2C failed");
        }
        if (!$fpm) {
            $ws = $class::newCaller()->open($target, "/ws");
            $ws->push("hello");
            $frame = $ws->recv(2);
            if (!$frame || $frame->data !== "hello") {
                throw new RuntimeException("websocket echo failed");
            }
            $ws->close();
        }
        if (
            $class::healthHandler()[0] !== 200 ||
            $class::healthHandler()[2] !== "OK"
        ) {
            throw new RuntimeException("default health");
        }
        $class::setHealthHandler(fn($request) => [503, [], "draining"]);
        if ($class::healthHandler()[0] !== 503) {
            throw new RuntimeException("custom health");
        }
    } finally {
        $class::stop();
    }
    echo "PASS PHP " . ($fpm ? "FPM" : "resident") . " -> Go HTTP\n";
};
if ($fpm) {
    $run();
} else {
    Swoole\Runtime::enableCoroutine(true);
    Swoole\Coroutine\run($run);
}
