package cc.apigo.discover;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DiscoverTest {

  @Test
  void contract() throws Exception {
    HttpServer server = HttpServer.create(
      new InetSocketAddress("127.0.0.1", 0),
      0
    );
    AtomicInteger code = new AtomicInteger(503);
    server.createContext(
      "/",
      Discover.middleware(exchange -> {
        exchange.getRequestBody().readAllBytes();
        exchange.sendResponseHeaders(code.get(), -1);
        exchange.close();
      })
    );
    server.start();
    String addr = "127.0.0.1:" + server.getAddress().getPort();
    String app = "discover-sdk-java-" + ProcessHandle.current().pid();
    Discover.Config cfg = new Discover.Config();
    cfg.app = app;
    cfg.calls = Map.of(app, "1");
    Discover.configure(cfg);
    try {
      Discover.start(addr);
      long deadline =
        System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (
          System.nanoTime() < deadline &&
          (!Discover.isRegistered() || Discover.getAppNodes(app).isEmpty())
        )
        new java.util.concurrent.CountDownLatch(1).await(
          10,
          java.util.concurrent.TimeUnit.MILLISECONDS
        );
      assertTrue(Discover.getAppNodes(app).containsKey(addr));
      assertThrows(java.io.IOException.class, () ->
        Discover.newCaller().post(app, "/submit", "payload".getBytes())
      );
      assertEquals(1, Discover.getAppNodes(app).get(addr).failedTimes);
      code.set(200);
      assertEquals(
        200,
        Discover.newCaller()
          .post(app, "/submit", "payload".getBytes())
          .statusCode()
      );
      assertEquals(0, Discover.getAppNodes(app).get(addr).failedTimes);
      code.set(501);
      assertEquals(
        501,
        Discover.newCaller()
          .post(app, "/submit", "payload".getBytes())
          .statusCode()
      );
      assertEquals(0, Discover.getAppNodes(app).get(addr).failedTimes);
      HttpClient client = HttpClient.newHttpClient();
      assertEquals(
        "OK",
        client
          .send(
            HttpRequest.newBuilder(
              URI.create("http://" + addr + Discover.healthPath)
            ).build(),
            HttpResponse.BodyHandlers.ofString()
          )
          .body()
      );
      Discover.setHealthHandler(exchange -> {
        exchange.sendResponseHeaders(503, -1);
        exchange.close();
      });
      assertEquals(
        503,
        client
          .send(
            HttpRequest.newBuilder(
              URI.create("http://" + addr + Discover.healthPath)
            ).build(),
            HttpResponse.BodyHandlers.discarding()
          )
          .statusCode()
      );
    } finally {
      Discover.setHealthHandler(null);
      Discover.stop();
      server.stop(0);
    }
  }

  @Test
  void crossLanguage() throws Exception {
    String target = System.getenv("DISCOVER_TEST_TARGET");
    if (target == null) return;
    String addr = System.getenv("DISCOVER_TEST_ADDR"),
      app = System.getenv("DISCOVER_TEST_PEER");
    Discover.Config cfg = new Discover.Config();
    cfg.app = app;
    cfg.calls = Map.of(target, "1");
    Discover.configure(cfg);
    try {
      Discover.start(addr);
      long deadline =
        System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (
          System.nanoTime() < deadline &&
          (!Discover.isRegistered() || Discover.getAppNodes(target).isEmpty())
        )
        new java.util.concurrent.CountDownLatch(1).await(
          10,
          java.util.concurrent.TimeUnit.MILLISECONDS
        );
      var context = new Discover.RequestContext(
        Map.of("X-App-Labels", List.of("a", "b")),
        "192.0.2.4",
        "entry",
        "http",
        "conformance"
      );
      var response = Discover.newCaller(context).post(
        target,
        "/echo",
        "payload".getBytes()
      );
      var data = new ObjectMapper().readTree(response.body());
      assertEquals("POST", data.get("method").asText());
      assertEquals("payload", data.get("body").asText());
      assertEquals(app, data.get("headers").get("X-From-App").get(0).asText());
      assertEquals(2, data.get("headers").get("X-App-Labels").size());
      Discover.addExternalApp(target, "2");
      long targetDeadline =
        System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (
          System.nanoTime() < targetDeadline &&
          Discover.getAppNodes(target).isEmpty()
        )
        new java.util.concurrent.CountDownLatch(1).await(
          10,
          java.util.concurrent.TimeUnit.MILLISECONDS
        );
      assertEquals(
        java.net.http.HttpClient.Version.HTTP_2,
        Discover.newCaller().get(target, "/echo").nativeResponse().version()
      );
      java.util.concurrent.CompletableFuture<String> echo =
        new java.util.concurrent.CompletableFuture<>();
      var ws = Discover.newCaller().open(
        target,
        "/ws",
        new java.net.http.WebSocket.Listener() {
          public void onOpen(java.net.http.WebSocket socket) {
            socket.request(1);
          }

          public java.util.concurrent.CompletionStage<?> onText(
            java.net.http.WebSocket socket,
            CharSequence data,
            boolean last
          ) {
            echo.complete(data.toString());
            socket.request(1);
            return null;
          }
        }
      );
      ws.sendText("hello", true).join();
      assertEquals("hello", echo.get(2, java.util.concurrent.TimeUnit.SECONDS));
      ws.sendClose(1000, "done").join();
      System.out.println("PASS Java -> Go HTTP/H2C/WebSocket");
    } finally {
      Discover.stop();
    }
  }
}
