<?php
require __DIR__ . "/../php/src/Discover.php";
require __DIR__ . "/../php/src/Caller.php";
require __DIR__ . "/../php-fpm/src/Discover.php";
use Apigo\Discover\Fpm\Discover as D;
$app = getenv("DISCOVER_TEST_APP");
D::configure([
    "app" => $app,
    "calls" => [$app . "-target" => "1"],
    "registry" => getenv("DISCOVER_TEST_REGISTRY"),
]);
$before = microtime(true);
D::start("127.0.0.1:31000");
$started = (microtime(true) - $before) * 1000;
$error = false;
try {
    D::fetchApp($app . "-target");
} catch (Throwable $exception) {
    $error = true;
}
echo json_encode([
    "startedMs" => $started,
    "registered" => D::isRegistered(),
    "server" => D::isServer(),
    "nodes" => count(D::getAppNodes($app . "-target")),
    "error" => $error,
]) . "\n";
D::stop();
