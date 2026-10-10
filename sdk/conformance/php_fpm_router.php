<?php
// PHP built-in request-scoped host exercises the same SDK lifecycle as FPM.
require __DIR__ . "/../php/src/Discover.php";
require __DIR__ . "/../php/src/Caller.php";
require __DIR__ . "/../php-fpm/src/Discover.php";
use Apigo\Discover\Fpm\Discover;
Discover::configure([
    "app" => getenv("DISCOVER_TEST_PEER"),
    "registry" => "redis://127.0.0.1:6379/15",
    "calls" => [],
]);
Discover::start();
try {
    [$code, $headers, $body] = Discover::middleware(
        fn($request) => [404, [], ""],
    )([
        "path" => parse_url($_SERVER["REQUEST_URI"], PHP_URL_PATH),
        "method" => $_SERVER["REQUEST_METHOD"],
    ]);
    http_response_code($code);
    foreach ($headers as $key => $value) {
        header($key . ": " . $value);
    }
    echo $body;
} finally {
    Discover::stop();
}
