<?php
namespace Apigo\Discover\Fpm;
/** FPM never owns registration/heartbeat and never unregisters a server. */
class Discover extends \Apigo\Discover\Discover
{
    public static function newCaller(
        $request = null,
        $logger = null
    ): \Apigo\Discover\Caller {
        if (!self::isClient()) {
            self::start();
        }
        return parent::newCaller($request, $logger);
    }
    public static function start(string $addr = ""): bool
    {
        self::$fpm = true;
        return parent::start("");
    }
}
