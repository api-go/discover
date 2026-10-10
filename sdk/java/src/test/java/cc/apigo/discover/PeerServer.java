package cc.apigo.discover;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class PeerServer {

  public static void main(String[] args) throws Exception {
    HttpServer server = HttpServer.create(
      new InetSocketAddress("127.0.0.1", 0),
      0
    );
    server.createContext(
      "/",
      Discover.middleware(exchange -> {
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
      })
    );
    server.start();
    String addr = "127.0.0.1:" + server.getAddress().getPort();
    Discover.Config cfg = new Discover.Config();
    cfg.app = System.getenv("DISCOVER_TEST_PEER");
    Discover.configure(cfg);
    Discover.start(addr);
    Runtime.getRuntime().addShutdownHook(
      new Thread(() -> {
        Discover.stop();
        server.stop(0);
      })
    );
    System.out.println("DISCOVER_READY " + addr);
    new CountDownLatch(1).await();
  }
}
