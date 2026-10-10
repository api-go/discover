"""Runs real Redis/HTTP interoperability checks; uses only prefixed test keys."""

import os, subprocess, tempfile, signal, select, time, sys
from pathlib import Path
from redis import Redis

ROOT = Path(__file__).resolve().parents[2]
PYTHON = os.environ.get("DISCOVER_PYTHON", sys.executable)
RUBY_ENV = os.environ.copy()
app = f"discover-conformance-{os.getpid()}"
rd = Redis.from_url("redis://127.0.0.1:6379/15", decode_responses=True)
rd.ping()
with tempfile.TemporaryDirectory(prefix="discover-conformance-") as tmp:
    binary = str(Path(tmp) / "server")
    client = str(Path(tmp) / "client")
    subprocess.run(
        ["go", "build", "-o", client, "./sdk/go/client"],
        cwd=ROOT,
        env={**os.environ, "GOWORK": "off"},
        check=True,
    )
    subprocess.run(
        ["go", "build", "-o", binary, "./sdk/go/server"],
        cwd=ROOT,
        env={**os.environ, "GOWORK": "off"},
        check=True,
    )
    server = subprocess.Popen(
        [binary],
        cwd=ROOT,
        env={**os.environ, "DISCOVER_TEST_APP": app},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        addr = None
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if not select.select([server.stdout], [], [], 0.5)[0]:
                continue
            line = server.stdout.readline()
            if line.startswith("DISCOVER_READY "):
                addr = line.strip().split(" ", 1)[1]
                break
            if server.poll() is not None:
                raise RuntimeError(server.stderr.read())
        if not addr:
            raise RuntimeError("Go service did not become ready")
        commands = [
            ("python", [PYTHON, "sdk/conformance/python_peer.py"], os.environ),
            ("node", ["node", "sdk/conformance/node_peer.mjs"], os.environ),
            ("java", ["mvn", "-q", "-f", "sdk/java/pom.xml", "test"], os.environ),
            ("ruby", ["ruby", "sdk/conformance/ruby_peer.rb"], RUBY_ENV),
            ("php", ["php", "sdk/conformance/php_peer.php"], os.environ),
            (
                "php-fpm",
                ["php", "sdk/conformance/php_peer.php"],
                {**os.environ, "DISCOVER_TEST_FPM": "1"},
            ),
        ]
        for name, command, baseEnv in commands:
            peer = app + "-" + name
            env = {
                **baseEnv,
                "DISCOVER_TEST_TARGET": app,
                "DISCOVER_TEST_PEER": peer,
                "DISCOVER_TEST_ADDR": addr,
            }
            subprocess.run(command, cwd=ROOT, env=env, check=True, timeout=45)
            assert rd.hlen(peer) == 0, f"{name} failed to unregister"
            assert not rd.exists(peer + "_" + addr), f"{name} leaked heartbeat"
        subprocess.run(
            [
                "mvn",
                "-q",
                "-f",
                "sdk/java/pom.xml",
                "dependency:build-classpath",
                "-Dmdep.outputFile=" + str(Path(tmp) / "java-classpath"),
            ],
            cwd=ROOT,
            check=True,
        )
        javaClasspath = (
            str(ROOT / "sdk/java/target/test-classes")
            + os.pathsep
            + str(ROOT / "sdk/java/target/classes")
            + os.pathsep
            + (Path(tmp) / "java-classpath").read_text().strip()
        )
        servers = [
            ("python", [PYTHON, "sdk/conformance/python_server.py"], os.environ),
            ("node", ["node", "sdk/conformance/node_server.mjs"], os.environ),
            ("ruby", ["ruby", "sdk/conformance/ruby_server.rb"], RUBY_ENV),
            (
                "java",
                ["java", "-cp", javaClasspath, "cc.apigo.discover.PeerServer"],
                os.environ,
            ),
            ("php", ["php", "sdk/conformance/php_server.php"], os.environ),
        ]
        for name, command, baseEnv in servers:
            peer = app + "-server-" + name
            process = subprocess.Popen(
                command,
                cwd=ROOT,
                env={**baseEnv, "DISCOVER_TEST_PEER": peer},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 10
                ready = False
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError(process.stderr.read())
                    if not select.select([process.stdout], [], [], 0.25)[0]:
                        continue
                    line = process.stdout.readline()
                    if line.startswith("DISCOVER_READY "):
                        ready = True
                        break
                if not ready:
                    raise RuntimeError(name + " server not ready")
                subprocess.run(
                    [client],
                    cwd=ROOT,
                    env={**os.environ, "DISCOVER_TEST_TARGET": peer},
                    check=True,
                    timeout=10,
                )
            finally:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            assert rd.hlen(peer) == 0, name + " server did not unregister"
        print("PASS all peer lifecycle cleanup; existing Redis left running")
    finally:
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
        for key in rd.scan_iter(match=app + "*"):
            rd.delete(key)
        rd.close()
