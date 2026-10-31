package cc.apigo.discover;

import java.util.*;
import java.util.concurrent.*;

public class RecoveryPeer {

  public static void main(String[] args) throws Exception {
    String app = System.getenv("DISCOVER_TEST_APP"),
      target = app + "-target";
    Discover.Config cfg = new Discover.Config();
    cfg.app = app;
    cfg.calls = Map.of(target, "1");
    cfg.registry = System.getenv("DISCOVER_TEST_REGISTRY");
    Discover.configure(cfg);
    long before = System.nanoTime();
    Discover.start("127.0.0.1:31000");
    System.out.println(
      "{\"startedMs\":" + (System.nanoTime() - before) / 1e6 + "}"
    );
    Runtime.getRuntime().addShutdownHook(
      new Thread(() -> {
        Discover.stop();
        System.out.println("{\"stopped\":true}");
      })
    );
    Boolean retained = null;
    Discover.config.callRetryTimes = 1;
    for (;;) {
      if (retained == null && !Discover.getAppNodes(target).isEmpty()) {
        try {
          Discover.newCaller().get(target, "/");
        } catch (Exception ignored) {}
        try (
          var checker = new redis.clients.jedis.Jedis(
            java.net.URI.create(cfg.registry),
            500
          )
        ) {
          retained = checker.hexists(target, "127.0.0.1:31999");
        }
      }
      System.out.println(
        "{\"globalMembershipRetained\":" +
          retained +
          ",\"registered\":" +
          Discover.isRegistered() +
          ",\"nodes\":" +
          Discover.getAppNodes(target).size() +
          "}"
      );
      new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
    }
  }
}
