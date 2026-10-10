package cc.apigo.discover;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import redis.clients.jedis.*;

/** Process-global Discover; request contexts and responses are request-local. */
public final class Discover {

  private Discover() {}

  public static final String healthPath = "/_discover/health";

  public static final class Config {

    public String registry = "redis://127.0.0.1:6379/15",
      app = "";
    public int weight = 100,
      callRetryTimes = 10;
    public Map<String, String> calls = new LinkedHashMap<>();
  }

  public static Config config = new Config();

  public static final class NodeInfo {

    public final String addr;
    public int weight;
    public long usedTimes;
    public int failedTimes;

    NodeInfo(String addr, int weight) {
      this.addr = addr;
      this.weight = weight;
    }

    NodeInfo copy() {
      NodeInfo n = new NodeInfo(addr, weight);
      n.usedTimes = usedTimes;
      n.failedTimes = failedTimes;
      return n;
    }
  }

  public record RequestContext(
    Map<String, List<String>> headers,
    String clientIP,
    String host,
    String scheme,
    String userAgent
  ) {
    public static RequestContext from(HttpExchange exchange) {
      return new RequestContext(
        exchange.getRequestHeaders(),
        exchange.getRemoteAddress().getAddress().getHostAddress(),
        exchange.getRequestHeaders().getFirst("Host"),
        exchange instanceof com.sun.net.httpserver.HttpsExchange
          ? "https"
          : "http",
        exchange.getRequestHeaders().getFirst("User-Agent")
      );
    }
  }

  public record Result(
    int statusCode,
    Map<String, List<String>> headers,
    byte[] body,
    String node,
    HttpResponse<byte[]> nativeResponse
  ) {}

  public static final class Call {

    public String method, app, path;
    public byte[] data;
    public Map<String, List<String>> headers;

    Call(
      String method,
      String app,
      String path,
      byte[] data,
      Map<String, List<String>> headers
    ) {
      this.method = method;
      this.app = app;
      this.path = path;
      this.data = data;
      this.headers = headers;
    }
  }

  public interface LoadBalancer {
    NodeInfo next(Call call, List<NodeInfo> nodes, RequestContext request);

    default void response(
      Call call,
      NodeInfo node,
      Exception error,
      Result result
    ) {}
  }

  private record CallInfo(
    Duration timeout,
    int httpVersion,
    boolean ssl,
    String token
  ) {}

  private static final Object lock = new Object();
  private static final Map<String, Map<String, NodeInfo>> nodes =
    new HashMap<>();
  private static final Map<String, CallInfo> calls = new HashMap<>();
  private static final Map<String, Future<?>> subscriptions = new HashMap<>();
  private static final Map<String, Jedis> subConnections =
    new ConcurrentHashMap<>();
  private static final Set<String> relay = Set.of(
    "x-real-ip",
    "x-forwarded-for",
    "x-user-id",
    "x-device-id",
    "x-client-app-name",
    "x-client-app-version",
    "x-session-id",
    "x-request-id",
    "x-host",
    "x-scheme",
    "x-user-agent"
  );
  private static volatile boolean running = false;
  private static boolean inited = false;
  private static String addr = "";
  private static JedisPool pool;
  private static ScheduledExecutorService executor;
  private static ExecutorService subscriberExecutor;
  private static final HttpClient http1 = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_1_1)
    .followRedirects(HttpClient.Redirect.NEVER)
    .build();
  private static final HttpClient http2 = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_2)
    .followRedirects(HttpClient.Redirect.NEVER)
    .build();
  private static Logger logger = Logger.getLogger("cc.apigo.discover");
  private static BiConsumer<Call, RequestContext> route;
  private static LoadBalancer lb;
  private static volatile HttpHandler health;
  private static final ThreadLocal<RequestContext> currentRequest =
    new ThreadLocal<>();

  public static synchronized void init() {
    if (inited) return;
    Map<String, String> env = System.getenv();
    config.registry = env.getOrDefault("DISCOVER_REGISTRY", config.registry);
    config.app = env.getOrDefault("DISCOVER_APP", config.app);
    if (env.containsKey("DISCOVER_WEIGHT")) config.weight = Integer.parseInt(
      env.get("DISCOVER_WEIGHT")
    );
    if (env.containsKey("DISCOVER_CALLS")) try {
      config.calls = new ObjectMapper().readValue(
        env.get("DISCOVER_CALLS"),
        new TypeReference<Map<String, String>>() {}
      );
    } catch (IOException e) {
      throw new IllegalArgumentException(
        "DISCOVER_CALLS must be a JSON map",
        e
      );
    }
    inited = true;
  }

  public static synchronized void configure(Config value) {
    init();
    config = value;
  }

  private static CallInfo parse(String conf) {
    long timeout = 10000;
    int version = 2;
    boolean ssl = false;
    String token = "";
    for (String part : conf.split(":"))
      switch (part) {
        case "1", "2" -> version = Integer.parseInt(part);
        case "s", "https" -> {
          ssl = true;
          version = 2;
        }
        case "http", "h2c" -> {
          ssl = false;
          version = part.equals("http") ? 1 : 2;
        }
        default -> {
          if (part.matches("\\d+(ms|s)?")) {
            timeout =
              Long.parseLong(part.replaceAll("ms|s$", "")) *
              (part.endsWith("ms") ? 1 : 1000);
          } else if (!part.isEmpty()) token = part;
        }
      }
    return new CallInfo(Duration.ofMillis(timeout), version, ssl, token);
  }

  public static void setNode(String app, String address, int weight) {
    synchronized (lock) {
      Map<String, NodeInfo> values = nodes.computeIfAbsent(app, k ->
        new LinkedHashMap<>()
      );
      if (weight <= 0) values.remove(address);
      else if (values.containsKey(address)) values.get(address).weight = weight;
      else values.put(address, new NodeInfo(address, weight));
    }
  }

  public static Map<String, NodeInfo> getAppNodes(String app) {
    synchronized (lock) {
      Map<String, NodeInfo> copy = new LinkedHashMap<>();
      nodes
        .getOrDefault(app, Map.of())
        .forEach((address, n) -> copy.put(address, n.copy()));
      return copy;
    }
  }

  private static void fetchApp(String app) {
    if (pool == null) return;
    Map<String, String> live = new HashMap<>();
    try (Jedis rd = pool.getResource()) {
      for (var entry : rd.hgetAll(app).entrySet())
        if (rd.exists(app + "_" + entry.getKey())) live.put(
          entry.getKey(),
          entry.getValue()
        );
    }
    synchronized (lock) {
      for (String address : new ArrayList<>(
        nodes.getOrDefault(app, Map.of()).keySet()
      ))
        if (!live.containsKey(address)) setNode(app, address, 0);
      live.forEach((address, weight) ->
        setNode(app, address, Integer.parseInt(weight))
      );
    }
  }

  public static boolean addExternalApp(String app, String callConf) {
    synchronized (lock) {
      calls.put(app, parse(callConf));
      nodes.computeIfAbsent(app, k -> new LinkedHashMap<>());
      if (running && !subscriptions.containsKey(app)) subscriptions.put(
        app,
        subscriberExecutor.submit(() -> subscribe(app))
      );
    }
    fetchApp(app);
    return true;
  }

  private static void subscribe(String app) {
    while (running) {
      try (Jedis rd = new Jedis(URI.create(config.registry))) {
        subConnections.put(app, rd);
        rd.subscribe(
          new JedisPubSub() {
            @Override
            public void onSubscribe(String channel, int count) {
              fetchApp(app);
            }

            @Override
            public void onMessage(String channel, String message) {
              String[] parts = message.split(" ");
              if (parts.length == 2) try {
                setNode(app, parts[0], Integer.parseInt(parts[1]));
              } catch (NumberFormatException ignored) {}
            }
          },
          "CH_" + app
        );
      } catch (Exception e) {
        if (running) {
          logger.warning("discover subscription: " + e);
          try {
            new CountDownLatch(1).await(250, TimeUnit.MILLISECONDS);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      } finally {
        subConnections.remove(app);
      }
    }
  }

  private static void register() {
    if (!isServer()) return;
    try (Jedis rd = pool.getResource()) {
      if (!rd.hexists(config.app, addr)) {
        rd.hset(config.app, addr, Integer.toString(config.weight));
        rd.setex(config.app + "_" + addr, 10, "1");
        rd.publish("CH_" + config.app, addr + " " + config.weight);
      } else rd.setex(config.app + "_" + addr, 10, "1");
    }
  }

  public static synchronized boolean start(String address) {
    init();
    stop();
    addr = address;
    pool = new JedisPool(URI.create(config.registry));
    try (Jedis rd = pool.getResource()) {
      rd.ping();
    }
    running = true;
    executor = Executors.newScheduledThreadPool(
      Math.max(2, config.calls.size() + 2),
      r -> {
        Thread t = new Thread(r, "discover");
        t.setDaemon(true);
        return t;
      }
    );
    subscriberExecutor = Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, "discover-sub");
      t.setDaemon(true);
      return t;
    });
    register();
    if (isServer()) executor.scheduleWithFixedDelay(
      () -> {
        try {
          register();
        } catch (Exception e) {
          logger.warning("discover heartbeat: " + e);
        }
      },
      1,
      1,
      TimeUnit.SECONDS
    );
    config.calls.forEach(Discover::addExternalApp);
    return true;
  }

  public static synchronized void stop() {
    running = false;
    subConnections.values().forEach(Jedis::close);
    subscriptions.clear();
    if (subscriberExecutor != null) {
      subscriberExecutor.shutdownNow();
      try {
        subscriberExecutor.awaitTermination(3, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      subscriberExecutor = null;
    }
    if (executor != null) {
      executor.shutdownNow();
      try {
        executor.awaitTermination(3, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      executor = null;
    }
    if (pool != null) {
      try {
        if (!addr.isEmpty() && !config.app.isEmpty()) try (
          Jedis rd = pool.getResource()
        ) {
          rd.hdel(config.app, addr);
          rd.del(config.app + "_" + addr);
          rd.publish("CH_" + config.app, addr + " 0");
        }
      } finally {
        pool.close();
        pool = null;
        addr = "";
      }
    }
  }

  public static void waitForStop() throws InterruptedException {
    while (running) new CountDownLatch(1).await(250, TimeUnit.MILLISECONDS);
  }

  public static boolean isServer() {
    return (
      running && !addr.isEmpty() && !config.app.isEmpty() && config.weight > 0
    );
  }

  public static boolean isClient() {
    return running;
  }

  public static void setLogger(Logger value) {
    logger = value;
  }

  public static void setRoute(BiConsumer<Call, RequestContext> value) {
    route = value;
  }

  public static void setLoadBalancer(LoadBalancer value) {
    lb = value;
  }

  public static void setHealthHandler(HttpHandler value) {
    health = value;
  }

  public static HttpHandler healthHandler() {
    return exchange -> {
      if (!exchange.getRequestMethod().equals("GET")) {
        exchange.getResponseHeaders().set("Allow", "GET");
        exchange.sendResponseHeaders(405, -1);
        exchange.close();
        return;
      }
      HttpHandler handler = health;
      if (handler != null) {
        handler.handle(exchange);
        return;
      }
      byte[] body = "OK".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      exchange
        .getResponseHeaders()
        .set("Content-Type", "text/plain; charset=utf-8");
      exchange.sendResponseHeaders(200, body.length);
      try (var stream = exchange.getResponseBody()) {
        stream.write(body);
      }
    };
  }

  public static HttpHandler middleware(HttpHandler next) {
    return exchange -> {
      if (
        exchange.getRequestURI().getPath().equals(healthPath)
      ) healthHandler().handle(exchange);
      else {
        if (
          exchange.getRequestHeaders().getFirst("X-Request-ID") == null
        ) exchange
          .getRequestHeaders()
          .set("X-Request-ID", UUID.randomUUID().toString().replace("-", ""));
        currentRequest.set(RequestContext.from(exchange));
        try {
          next.handle(exchange);
        } finally {
          currentRequest.remove();
        }
      }
    };
  }

  public static Caller newCaller(RequestContext request) {
    return new Caller(request);
  }

  public static Caller newCaller() {
    return new Caller(currentRequest.get());
  }

  private static Map<String, List<String>> headers(RequestContext request) {
    Map<String, List<String>> result = new TreeMap<>(
      String.CASE_INSENSITIVE_ORDER
    );
    if (request != null) {
      request.headers().forEach((name, values) -> {
        if (
          relay.contains(name.toLowerCase(Locale.ROOT)) ||
          name.toLowerCase(Locale.ROOT).startsWith("x-app-")
        ) result.put(name, new ArrayList<>(values));
      });
      putMissing(result, "X-Real-IP", request.clientIP());
      putMissing(result, "X-Host", request.host());
      putMissing(result, "X-Scheme", request.scheme());
      putMissing(result, "X-User-Agent", request.userAgent());
    }
    putMissing(
      result,
      "X-Request-ID",
      UUID.randomUUID().toString().replace("-", "")
    );
    return result;
  }

  private static void putMissing(
    Map<String, List<String>> hs,
    String key,
    String value
  ) {
    if (value != null && !value.isEmpty()) hs.putIfAbsent(key, List.of(value));
  }

  public static final class Caller {

    private final RequestContext request;

    Caller(RequestContext request) {
      this.request = request;
    }

    // "do" and "wait" are reserved in Java; doRequest and waitForStop are explicit exceptions.
    public Result doRequest(
      String method,
      String app,
      String path,
      byte[] data,
      Map<String, List<String>> explicit
    ) throws IOException, InterruptedException {
      return doWithNode(method, app, "", path, data, explicit);
    }

    public Result doWithNode(
      String method,
      String app,
      String withNode,
      String path,
      byte[] data,
      Map<String, List<String>> explicit
    ) throws IOException, InterruptedException {
      Call call = new Call(
        method,
        app,
        path,
        data,
        explicit == null ? Map.of() : explicit
      );
      if (route != null) route.accept(call, request);
      app = call.app;
      path = call.path;
      if (
        !path.startsWith("/") || path.startsWith("//")
      ) throw new IllegalArgumentException("path must be local absolute");
      synchronized (lock) {
        if (!calls.containsKey(app)) addExternalApp(app, "");
      }
      CallInfo info;
      synchronized (lock) {
        info = calls.get(app);
      }
      Map<String, List<String>> hs = headers(request);
      hs.putAll(call.headers);
      if (isServer()) {
        hs.put("X-From-App", List.of(config.app));
        hs.put("X-From-Node", List.of(addr));
      }
      if (!info.token.isEmpty()) hs.put("Access-Token", List.of(info.token));
      Set<String> excluded = new HashSet<>();
      long deadline = System.nanoTime() + info.timeout.toNanos();
      IOException last = null;
      while (true) {
        if (
          Thread.currentThread().isInterrupted()
        ) throw new InterruptedException();
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new HttpTimeoutException(
          "discover total timeout"
        );
        NodeInfo node;
        synchronized (lock) {
          List<NodeInfo> all = new ArrayList<>();
          for (NodeInfo n : nodes.get(app).values())
            if (
              !excluded.contains(n.addr) &&
              (withNode.isEmpty() || withNode.equals(n.addr))
            ) all.add(n);
          List<NodeInfo> ready = all
            .stream()
            .filter(n -> n.failedTimes < config.callRetryTimes)
            .toList();
          if (ready.isEmpty()) ready = all;
          if (ready.isEmpty()) throw new IOException(
            "all nodes failed for " + app,
            last
          );
          node =
            lb != null
              ? lb.next(call, ready, request)
              : ready
                  .stream()
                  .min(
                    Comparator.comparingDouble(
                      n -> (double) n.usedTimes / n.weight
                    )
                  )
                  .orElseThrow();
          node.usedTimes++;
          excluded.add(node.addr);
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(
          URI.create((info.ssl ? "https" : "http") + "://" + node.addr + path)
        )
          .timeout(Duration.ofNanos(remaining))
          .method(
            call.method,
            call.data == null
              ? HttpRequest.BodyPublishers.noBody()
              : HttpRequest.BodyPublishers.ofByteArray(call.data)
          );
        hs.forEach((name, values) ->
          values.forEach(value -> builder.header(name, value))
        );
        Result result = null;
        try {
          HttpResponse<byte[]> response = (
            info.httpVersion == 2 ? http2 : http1
          ).send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
          result = new Result(
            response.statusCode(),
            response.headers().map(),
            response.body(),
            node.addr,
            response
          );
          last = null;
        } catch (IOException error) {
          last = error;
        }
        boolean failed =
          last != null ||
          (result.statusCode >= 502 && result.statusCode <= 504);
        int failures;
        synchronized (lock) {
          node.failedTimes = failed ? node.failedTimes + 1 : 0;
          failures = node.failedTimes;
        }
        if (lb != null) lb.response(call, node, last, result);
        if (!failed) return result;
        if (failures >= config.callRetryTimes && pool != null) try (
          Jedis rd = pool.getResource()
        ) {
          rd.hdel(app, node.addr);
          rd.publish("CH_" + app, node.addr + " 0");
        }
      }
    }

    public WebSocket open(String app, String path, WebSocket.Listener listener)
      throws IOException, InterruptedException {
      if (
        !path.startsWith("/") || path.startsWith("//")
      ) throw new IllegalArgumentException("path must be local absolute");
      synchronized (lock) {
        if (!calls.containsKey(app)) addExternalApp(app, "");
      }
      CallInfo info;
      synchronized (lock) {
        info = calls.get(app);
      }
      Map<String, List<String>> hs = headers(request);
      if (isServer()) {
        hs.put("X-From-App", List.of(config.app));
        hs.put("X-From-Node", List.of(addr));
      }
      if (!info.token.isEmpty()) hs.put("Access-Token", List.of(info.token));
      Set<String> excluded = new HashSet<>();
      long deadline = System.nanoTime() + info.timeout.toNanos();
      while (true) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new HttpTimeoutException(
          "websocket handshake timeout"
        );
        NodeInfo node;
        synchronized (lock) {
          List<NodeInfo> ready = nodes
            .get(app)
            .values()
            .stream()
            .filter(n -> !excluded.contains(n.addr))
            .toList();
          if (ready.isEmpty()) throw new IOException(
            "all websocket nodes failed"
          );
          node = ready
            .stream()
            .min(
              Comparator.comparingDouble(n -> (double) n.usedTimes / n.weight)
            )
            .orElseThrow();
          node.usedTimes++;
          excluded.add(node.addr);
        }
        var builder = http1
          .newWebSocketBuilder()
          .connectTimeout(Duration.ofNanos(remaining));
        hs.forEach((name, values) ->
          values.forEach(value -> builder.header(name, value))
        );
        try {
          var connection = builder
            .buildAsync(
              URI.create((info.ssl ? "wss" : "ws") + "://" + node.addr + path),
              listener
            )
            .get(remaining, TimeUnit.NANOSECONDS);
          synchronized (lock) {
            node.failedTimes = 0;
          }
          return connection;
        } catch (ExecutionException e) {
          if (
            e.getCause() instanceof WebSocketHandshakeException handshake &&
            !(
              handshake.getResponse().statusCode() >= 502 &&
              handshake.getResponse().statusCode() <= 504
            )
          ) throw new IOException("websocket rejected", e);
          synchronized (lock) {
            node.failedTimes++;
          }
        } catch (TimeoutException e) {
          throw new HttpTimeoutException("websocket handshake timeout");
        }
      }
    }

    public Result get(String app, String path)
      throws IOException, InterruptedException {
      return doRequest("GET", app, path, null, null);
    }

    public Result post(String app, String path, byte[] body)
      throws IOException, InterruptedException {
      return doRequest("POST", app, path, body, null);
    }

    public Result put(String app, String path, byte[] body)
      throws IOException, InterruptedException {
      return doRequest("PUT", app, path, body, null);
    }

    public Result delete(String app, String path, byte[] body)
      throws IOException, InterruptedException {
      return doRequest("DELETE", app, path, body, null);
    }

    public Result head(String app, String path)
      throws IOException, InterruptedException {
      return doRequest("HEAD", app, path, null, null);
    }
  }
}
