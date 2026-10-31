<?php
require __DIR__ . "/../php/src/Discover.php";
require __DIR__ . "/../php/src/Caller.php";
use Apigo\Discover\Discover as D;
Swoole\Runtime::enableCoroutine(true);
Swoole\Coroutine\run(function () {
    $app = getenv("DISCOVER_TEST_APP");
    $target = $app . "-target";
    D::configure([
        "app" => $app,
        "calls" => [$target => "1"],
        "registry" => getenv("DISCOVER_TEST_REGISTRY"),
    ]);
    $before = microtime(true);
    D::start("127.0.0.1:31000");
    echo json_encode(["startedMs" => (microtime(true) - $before) * 1000]) .
        "\n";
    D::$config["callRetryTimes"] = 1;
    $retained = null;
    $checking = false;
    $tick = Swoole\Timer::tick(100, function () use (
        $target,
        &$retained,
        &$checking
    ) {
        if ($retained === null && !$checking && D::getAppNodes($target)) {
            $checking = true;
            Swoole\Coroutine::create(function () use ($target, &$retained) {
                try {
                    D::newCaller()->get($target, "/");
                } catch (Throwable $ignored) {
                }
                $retained = D::redis(
                    fn($rd) => $rd->hExists($target, "127.0.0.1:31999"),
                );
            });
        }
        echo json_encode([
            "globalMembershipRetained" => $retained,
            "registered" => D::isRegistered(),
            "nodes" => count(D::getAppNodes($target)),
            "health" => D::healthHandler()[0],
        ]) . "\n";
    });
    Swoole\Process::signal(SIGTERM, function () use ($tick) {
        Swoole\Timer::clear($tick);
        Swoole\Coroutine::create(function () {
            D::stop();
            echo json_encode(["stopped" => true]) . "\n";
        });
    });
});
