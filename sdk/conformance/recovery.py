"""Isolated Redis fault injection. No user Redis is touched; no timing guarantee inferred."""

import json, os, queue, signal, socket, subprocess, sys, tempfile, threading, time
from pathlib import Path
import redis

ROOT = Path(__file__).resolve().parents[2]


def until(check, seconds=12):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (redis.RedisError, OSError):
            pass
        threading.Event().wait(0.05)
    raise AssertionError("condition did not recover within test deadline")


with tempfile.TemporaryDirectory(prefix="go-discover-recovery-") as tmp:
    subprocess.run(
        ["go", "build", "-o", tmp + "/go-peer", "./sdk/go/recovery"],
        cwd=ROOT,
        env={**os.environ, "GOWORK": "off"},
        check=True,
    )
    subprocess.run(
        [
            "mvn",
            "-q",
            "-f",
            "sdk/java/pom.xml",
            "test-compile",
            "dependency:build-classpath",
            "-Dmdep.outputFile=" + tmp + "/classpath",
        ],
        cwd=ROOT,
        check=True,
    )
    cp = (
        str(ROOT / "sdk/java/target/test-classes")
        + os.pathsep
        + str(ROOT / "sdk/java/target/classes")
        + os.pathsep
        + Path(tmp + "/classpath").read_text().strip()
    )
    commands = {
        "go": [tmp + "/go-peer"],
        "python": [sys.executable, "sdk/conformance/recovery_python.py"],
        "node": ["node", "sdk/conformance/recovery_node.mjs"],
        "java": ["java", "-cp", cp, "cc.apigo.discover.RecoveryPeer"],
        "ruby": [
            os.environ.get("DISCOVER_RUBY", "ruby"),
            "sdk/conformance/recovery_ruby.rb",
        ],
        "php": [
            os.environ.get("DISCOVER_PHP", "php"),
            "sdk/conformance/recovery_php.php",
        ],
    }
    for name in sys.argv[1:] or commands:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        app = f"go-discover-recovery-{os.getpid()}-{name}"
        target = app + "-target"
        address = "127.0.0.1:31000"
        rd = redis.Redis(
            host="127.0.0.1",
            port=port,
            decode_responses=True,
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
        )
        server = None
        peer = None
        records = queue.Queue()
        latest = {}

        def startRedis():
            global server
            server = subprocess.Popen(
                [
                    "redis-server",
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                    "--dir",
                    tmp,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            until(lambda: rd.ping())

        def stopRedis():
            global server
            if server:
                server.kill()
                server.wait()
                server = None

        def status(predicate):
            while True:
                try:
                    latest.update(records.get_nowait())
                except queue.Empty:
                    break
            if peer.poll() is not None and not latest.get("stopped"):
                raise AssertionError(f"{name} exited: {peer.returncode}")
            return predicate(latest)

        try:
            with open(tmp + "/" + name + ".log", "w") as errors:
                peer = subprocess.Popen(
                    commands[name],
                    cwd=ROOT,
                    env={
                        **os.environ,
                        "DISCOVER_TEST_APP": app,
                        "DISCOVER_TEST_REGISTRY": f"redis://127.0.0.1:{port}/0",
                    },
                    stdout=subprocess.PIPE,
                    stderr=errors,
                    text=True,
                )

                def collect():
                    for line in peer.stdout:
                        try:
                            records.put(json.loads(line))
                        except ValueError:
                            pass

                reader = threading.Thread(target=collect, daemon=True)
                reader.start()
                until(lambda: status(lambda value: "startedMs" in value))
                assert latest["startedMs"] < 1000, latest
                until(lambda: status(lambda value: value.get("registered") is False))
                # Wait through multiple retry intervals with no Redis.
                threading.Event().wait(5)
                startRedis()
                until(lambda: rd.hexists(app, address))
                until(lambda: status(lambda value: value.get("registered") is True))
                # No PUBLISH: periodic reconciliation must discover this record.
                rd.eval(
                    "redis.call('HSET',KEYS[1],ARGV[1],100);redis.call('SETEX',KEYS[2],30,'1');return 1",
                    2,
                    target,
                    target + "_127.0.0.1:31999",
                    "127.0.0.1:31999",
                )
                until(lambda: status(lambda value: value.get("nodes") == 1))
                until(
                    lambda: status(
                        lambda value: value.get("globalMembershipRetained") is True
                    )
                )
                stopRedis()
                until(lambda: status(lambda value: value.get("registered") is False))
                threading.Event().wait(11)  # longer than the 10-second lease
                assert status(
                    lambda value: value.get("nodes") == 1
                ), "failed snapshot discarded cached nodes"
                startRedis()
                until(lambda: rd.hexists(app, address))
                until(
                    lambda: status(
                        lambda value: value.get("registered") is True
                        and value.get("nodes") == 0
                    )
                )
                peer.send_signal(signal.SIGTERM)
                peer.wait(timeout=8)
                reader.join(timeout=2)
                assert status(lambda value: value.get("stopped") is True), latest
                assert not rd.hexists(app, address), "Stop left membership behind"
                threading.Event().wait(2)
                assert not rd.hexists(
                    app, address
                ), "old worker re-registered after Stop"
                # Stop a freshly started worker while Redis is still absent.
                stopRedis()
                latest.clear()
                records = queue.Queue()
                peer = subprocess.Popen(
                    commands[name],
                    cwd=ROOT,
                    env={
                        **os.environ,
                        "DISCOVER_TEST_APP": app,
                        "DISCOVER_TEST_REGISTRY": f"redis://127.0.0.1:{port}/0",
                    },
                    stdout=subprocess.PIPE,
                    stderr=errors,
                    text=True,
                )
                reader = threading.Thread(target=collect, daemon=True)
                reader.start()
                until(lambda: status(lambda value: "startedMs" in value))
                assert latest["startedMs"] < 1000, latest
                until(lambda: status(lambda value: value.get("registered") is False))
                peer.send_signal(signal.SIGTERM)
                peer.wait(timeout=8)
                reader.join(timeout=2)
                assert status(lambda value: value.get("stopped") is True), latest
                startRedis()
                threading.Event().wait(2)
                assert not rd.hexists(
                    app, address
                ), "worker survived Stop during outage"
                if name == "php":

                    def fpmRequest():
                        result = subprocess.run(
                            [
                                os.environ.get("DISCOVER_PHP", "php"),
                                "sdk/conformance/recovery_fpm.php",
                            ],
                            cwd=ROOT,
                            env={
                                **os.environ,
                                "DISCOVER_TEST_APP": app,
                                "DISCOVER_TEST_REGISTRY": f"redis://127.0.0.1:{port}/0",
                            },
                            capture_output=True,
                            text=True,
                            timeout=5,
                            check=True,
                        )
                        value = json.loads(result.stdout)
                        assert (
                            value["startedMs"] < 1000
                            and not value["registered"]
                            and not value["server"]
                        ), value
                        return value

                    stopRedis()
                    assert fpmRequest()["error"]
                    startRedis()
                    rd.hset(target, "127.0.0.1:31999", 100)
                    rd.set(target + "_127.0.0.1:31999", "1", ex=10)
                    assert fpmRequest()["nodes"] == 1
                    assert not rd.hexists(app, address), "FPM registered a service"
                    print(
                        "PASS php-fpm request initialization without Redis, request recovery, no lifecycle ownership",
                        flush=True,
                    )
                print(
                    "PASS",
                    name,
                    "nonblocking startup, delayed Redis, missed notification, >lease outage, empty restart, Stop",
                    flush=True,
                )
        except Exception:
            if Path(tmp + "/" + name + ".log").exists():
                print(
                    Path(tmp + "/" + name + ".log").read_text()[-6000:], file=sys.stderr
                )
            raise
        finally:
            if peer and peer.poll() is None:
                peer.kill()
                peer.wait()
            stopRedis()
            rd.close()
