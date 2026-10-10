<?php
require __DIR__ . "/../php/src/Discover.php";
require __DIR__ . "/../php/src/Caller.php";
use Apigo\Discover\Discover;
Swoole\Runtime::enableCoroutine(true);
Swoole\Coroutine\run(function () {
    $server = new Swoole\Coroutine\Http\Server("127.0.0.1", 0, false);
    $server->handle(
        "/",
        Discover::swooleMiddleware(function ($request, $response) {
            $response->status(404);
            $response->end();
        }),
    );
    $addr = "127.0.0.1:" . $server->port;
    Discover::configure([
        "app" => getenv("DISCOVER_TEST_PEER"),
        "registry" => "redis://127.0.0.1:6379/15",
        "calls" => [],
    ]);
    Discover::start($addr);
    Swoole\Process::signal(SIGTERM, function () use ($server) {
        Discover::stop();
        $server->shutdown();
    });
    echo "DISCOVER_READY " . $addr . "\n";
    try {
        $server->start();
    } finally {
        Discover::stop();
    }
});
