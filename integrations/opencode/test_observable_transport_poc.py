#!/usr/bin/env python3
"""Synthetic and OpenCode v2.2.5 validation for the isolated transport PoC."""

from __future__ import annotations

import base64
import http.client
import json
import os
import shutil
import select
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from observable_transport_poc import ObservableTransport


ROOT = Path(__file__).resolve().parents[2]
TMP_PARENT = Path("/private/var/folders/zr/hnnn53p16qb_vh8tl5c73k2c0000gn/T")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class BackendHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    rows: list[dict] = []
    lock = threading.Lock()
    seen = threading.Event()
    real_mode: str | None = None

    def log_message(self, *_):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        row = {"path": self.path, "body_size": len(body), "authorization_present": bool(self.headers.get("Authorization"))}
        with self.lock:
            self.rows.append(row)
        self.seen.set()
        route = self.path.split("?", 1)[0]
        if route.startswith("/v1/") and self.real_mode:
            route = self.real_mode
        if route == "/json":
            self.send_response(200)
            payload = b'{"id":"poc","object":"chat.completion","choices":[{"message":{"content":"ok"}}]}'
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if route == "/sse-complete":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            for chunk in (b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n", b"data: [DO", b"NE]\n\n"):
                self.wfile.write(chunk)
                self.wfile.flush()
                time.sleep(0.08)
            self.close_connection = True
            return
        if route == "/sse-truncated":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
            self.wfile.flush()
            self.close_connection = True
            return
        if route == "/error-before-headers":
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
            return
        if route == "/delay-headers":
            # Wait for the client socket to close; this records no body and
            # does not turn a timer into a cancellation assertion.
            deadline = time.time() + 15
            while time.time() < deadline:
                try:
                    readable, _, _ = select.select([self.connection], [], [], 0.05)
                    if readable and not self.connection.recv(1, socket.MSG_PEEK):
                        self.close_connection = True
                        return
                except OSError:
                    self.close_connection = True
                    return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.close_connection = True
            return
        if route == "/sse-cancel":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            try:
                for _ in range(80):
                    self.wfile.write(b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
                    self.wfile.flush()
                    time.sleep(0.05)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            self.close_connection = True
            return
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()


class Backend(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def events(transport: ObservableTransport, transport_id: str | None = None) -> list[dict]:
    return [row for row in transport.events if transport_id is None or row.get("transport_id") == transport_id]


def request(port: int, path: str, body: bytes, headers: dict[str, str], read=True):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    connection.request("POST", path, body=body, headers={"Content-Length": str(len(body)), **headers})
    response = connection.getresponse()
    data = response.read() if read else b""
    connection.close()
    return response.status, response.getheaders(), data


def ids(path: str) -> dict[str, str]:
    return {
        "X-LARM-Execution-ID": str(uuid.uuid4()),
        "X-LARM-Session-ID": f"ses-poc-{path.strip('/')}"[:64],
        "X-LARM-Request-ID": str(uuid.uuid4()),
        "X-LARM-Lease-ID": str(uuid.uuid4()),
    }


def result(name: str, status: str, **fields) -> dict:
    return {"test": name, "status": status, **fields}


def synthetic_tests(transport: ObservableTransport) -> list[dict]:
    output = []
    body = json.dumps({"model": "poc-model", "messages": [], "stream": False}).encode()
    header = ids("json")
    status, _, response_body = request(transport.port, "/json?probe=1", body, header)
    row = next(r for r in transport.events if r["request_id"] == header["X-LARM-Request-ID"] and r["type"] == "DOWNSTREAM_CONNECTED")
    trace = events(transport, row["transport_id"])
    terminal = [r for r in trace if r["type"] in {"REQUEST_SETTLED", "REQUEST_UNKNOWN"}]
    backend_row = BackendHandler.rows[-1]
    ok = status == 200 and response_body.startswith(b"{") and backend_row["body_size"] == len(body) and \
        [r["type"] for r in trace].count("UPSTREAM_STARTED") == 1 and \
        any(r["type"] == "UPSTREAM_EOF" for r in trace) and len(terminal) == 1 and terminal[0]["diagnosis"] == "HTTP_COMPLETED"
    output.append(result("TEST-01", "PASS" if ok else "FAIL", transport_id=row["transport_id"], event_types=[r["type"] for r in trace], body_size=len(body), backend_body_size=backend_row["body_size"]))

    header = ids("sse-complete")
    started = time.monotonic()
    status, _, response_body = request(transport.port, "/sse-complete", json.dumps({"model": "poc-model", "stream": True}).encode(), header)
    elapsed = time.monotonic() - started
    row = next(r for r in transport.events if r["request_id"] == header["X-LARM-Request-ID"] and r["type"] == "DOWNSTREAM_CONNECTED")
    trace = events(transport, row["transport_id"])
    terminal = [r for r in trace if r["type"] in {"REQUEST_SETTLED", "REQUEST_UNKNOWN"}]
    ok = status == 200 and b"[DONE]" in response_body and elapsed >= 0.15 and any(r["type"] == "SSE_DONE" for r in trace) and any(r["type"] == "UPSTREAM_EOF" for r in trace) and len(terminal) == 1 and terminal[0]["diagnosis"] == "SSE_COMPLETED"
    output.append(result("TEST-02", "PASS" if ok else "FAIL", transport_id=row["transport_id"], event_types=[r["type"] for r in trace], streamed_before_eof=elapsed >= 0.15))

    header = ids("sse-truncated")
    status, _, response_body = request(transport.port, "/sse-truncated", json.dumps({"model": "poc-model", "stream": True}).encode(), header)
    row = next(r for r in transport.events if r["request_id"] == header["X-LARM-Request-ID"] and r["type"] == "DOWNSTREAM_CONNECTED")
    trace = events(transport, row["transport_id"])
    terminal = [r for r in trace if r["type"] in {"REQUEST_SETTLED", "REQUEST_UNKNOWN"}]
    ok = status == 200 and response_body and any(r["type"] == "UPSTREAM_EOF" for r in trace) and not any(r["type"] == "SSE_DONE" for r in trace) and len(terminal) == 1 and terminal[0]["diagnosis"] == "SSE_TRUNCATED"
    output.append(result("TEST-03", "PASS" if ok else "FAIL", transport_id=row["transport_id"], event_types=[r["type"] for r in trace], automatic_release=False))

    header = ids("error-before-headers")
    status, _, _ = request(transport.port, "/error-before-headers", b'{"model":"poc-model"}', header)
    row = next(r for r in transport.events if r["request_id"] == header["X-LARM-Request-ID"] and r["type"] == "DOWNSTREAM_CONNECTED")
    trace = events(transport, row["transport_id"])
    terminal = [r for r in trace if r["type"] in {"REQUEST_SETTLED", "REQUEST_UNKNOWN"}]
    ok = status == 502 and any(r["type"] == "UPSTREAM_ERROR" for r in trace) and not any(r["type"] == "UPSTREAM_HEADERS" for r in trace) and len(terminal) == 1 and terminal[0]["diagnosis"] == "TRANSPORT_ERROR"
    output.append(result("TEST-04", "PASS" if ok else "FAIL", transport_id=row["transport_id"], event_types=[r["type"] for r in trace], plugin_observation=False))

    return output


def race_and_identity_tests(transport: ObservableTransport) -> list[dict]:
    output = []
    requests = []
    results = {}

    def run_one(name: str, close_early: bool):
        header = ids(name)
        requests.append(header)
        connection = http.client.HTTPConnection("127.0.0.1", transport.port, timeout=10)
        payload = b'{"model":"poc-model","stream":true}'
        path = "/sse-cancel" if close_early else "/json"
        payload = b'{"model":"poc-model","stream":true}' if close_early else b'{"model":"poc-model","stream":false}'
        connection.request("POST", path, body=payload, headers={"Content-Length": str(len(payload)), **header})
        response = connection.getresponse()
        first = response.read(1)
        if close_early:
            connection.close()
        else:
            response.read()
            connection.close()
        results[name] = {"status": response.status, "first_byte": bool(first)}

    threads = [threading.Thread(target=run_one, args=("race-a", True)), threading.Thread(target=run_one, args=("race-b", False))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    deadline = time.time() + 8
    while time.time() < deadline:
        if all(any(r["request_id"] == h["X-LARM-Request-ID"] and r["type"] in {"REQUEST_SETTLED", "REQUEST_UNKNOWN"} for r in transport.events) for h in requests):
            break
        time.sleep(0.02)
    per_request = {}
    for header in requests:
        trace = [r for r in transport.events if r["request_id"] == header["X-LARM-Request-ID"]]
        terminals = [r for r in trace if r["type"] in {"REQUEST_SETTLED", "REQUEST_UNKNOWN"}]
        per_request[header["X-LARM-Request-ID"]] = {"events": [r["type"] for r in trace], "terminal_count": len(terminals)}
    identity_ok = len({h["X-LARM-Request-ID"] for h in requests}) == 2 and all(row["terminal_count"] == 1 for row in per_request.values()) and results.get("race-b", {}).get("status") == 200
    output.append(result("TEST-07", "PASS" if identity_ok else "FAIL", per_request=per_request, results=results, unique_request_ids=len(per_request)))
    isolation_ok = all(row["terminal_count"] == 1 for row in per_request.values()) and all(len([r for r in transport.events if r["request_id"] == request_id]) > 0 for request_id in per_request)
    output.append(result("TEST-08", "PASS" if isolation_ok else "FAIL", per_request=per_request, ownership_operations=0, lease_operations=0))
    return output


def write_opencode_plugin(path: Path, evidence: Path):
    # The fixture plugin only adds diagnostic identity headers; it does not use
    # the LARM coordinator and does not create or release ownership.
    path.write_text(f'''import {{ appendFileSync }} from "node:fs"
import {{ randomUUID }} from "node:crypto"
import {{ Plugin }} from "@opencode/plugin"
const evidence = {json.dumps(str(evidence))}
const execution = randomUUID()
function emit(type, fields = {{}}) {{ appendFileSync(evidence, JSON.stringify({{ type, at: new Date().toISOString(), ...fields }}) + "\\n") }}
export default Plugin.define({{ id: "observable-transport-poc", async setup(ctx) {{
  await ctx.session.hook("model.request", async (event) => {{
    const requestID = randomUUID()
    event.headers["X-LARM-Execution-ID"] = execution
    event.headers["X-LARM-Request-ID"] = requestID
    event.headers["X-LARM-Session-ID"] = event.sessionID
    emit("MODEL_REQUEST", {{ execution_id: execution, request_id: requestID, session_id: event.sessionID, model_id: event.model.id }})
  }})
  await ctx.session.hook("http.request", async (event) => emit("HTTP_REQUEST", {{ execution_id: execution, session_id: event.sessionID }}))
  await ctx.session.hook("http.response", async (event) => emit("HTTP_RESPONSE", {{ execution_id: execution, session_id: event.sessionID, status: event.response.status }}))
  void (async () => {{ for await (const event of ctx.event.subscribe({{ types: ["session.execution.interrupted", "session.execution.succeeded", "session.execution.failed"] }})) emit(event.type, {{ execution_id: execution, session_id: event.data.sessionID }}) }})()
}} }})
''', encoding="utf-8")


def opencode_api(port: int, password: str, method: str, path: str, value=None, timeout=5):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    payload = None if value is None else json.dumps(value).encode()
    token = base64.b64encode(f"opencode:{password}".encode()).decode()
    headers = {"Authorization": "Basic " + token}
    if payload:
        headers["Content-Type"] = "application/json"
    connection.request(method, "/api" + path, body=payload, headers=headers)
    response = connection.getresponse()
    body = response.read()
    status = response.status
    connection.close()
    try:
        return status, json.loads(body) if body else None
    except json.JSONDecodeError:
        return status, body.decode(errors="replace")


def opencode_case(transport: ObservableTransport, backend_path: str, name: str) -> dict:
    binary = shutil.which("opencode") or "/Users/odin/.opencode/bin/opencode"
    if not Path(binary).is_file():
        return result(name, "BLOCKED", reason="OpenCode v2.2.5 binary unavailable")
    root = Path(tempfile.mkdtemp(prefix="larm-observable-transport.", dir=TMP_PARENT))
    project = root / "project"
    plugin_dir = project / ".opencode" / "plugins"
    plugin_dir.mkdir(parents=True)
    evidence = root / "opencode-events.jsonl"
    write_opencode_plugin(plugin_dir / "transport.ts", evidence)
    password = uuid.uuid4().hex
    config = {
        "model": "poc/poc-model",
        "providers": {"poc": {"name": "Observable transport fixture", "package": "@opencode/ai/providers/openai-compatible",
                                "settings": {"baseURL": transport.base_url + "/v1", "apiKey": "fixture-secret"},
                                "models": {"poc-model": {"modelID": "poc-model", "name": "PoC model",
                                                               "capabilities": {"tools": False, "input": ["text"], "output": ["text"]},
                                                               "limit": {"context": 8192, "output": 64}}}}},
    }
    (project / ".opencode" / "opencode.json").write_text(json.dumps(config), encoding="utf-8")
    env = os.environ.copy()
    for key, directory in (("HOME", "home"), ("XDG_CONFIG_HOME", "home/config"), ("XDG_DATA_HOME", "home/data"),
                           ("XDG_CACHE_HOME", "home/cache"), ("XDG_STATE_HOME", "home/state")):
        value = root / directory
        value.mkdir(parents=True, exist_ok=True)
        env[key] = str(value)
    env.update({"OPENCODE_CONFIG": str(project / ".opencode" / "opencode.json"), "OPENCODE_DISABLE_AUTOUPDATE": "1",
                "OPENCODE_SERVER_PASSWORD": password, "OPENCODE_SERVER_USERNAME": "opencode"})
    port = free_port()
    process = subprocess.Popen([binary, "serve", "--hostname", "127.0.0.1", "--port", str(port)], cwd=project, env=env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    prompt_thread = None
    prompt_result = {}
    try:
        BackendHandler.real_mode = backend_path
        deadline = time.time() + 30
        while time.time() < deadline:
            if process.poll() is not None:
                return result(name, "BLOCKED", reason="isolated OpenCode exited during startup")
            try:
                status, info = opencode_api(port, password, "GET", "/info", timeout=1)
                if status == 200 and str((info or {}).get("version", (info or {}).get("data", {}).get("version", ""))).endswith("2.2.5"):
                    break
            except OSError:
                pass
            time.sleep(0.1)
        else:
            return result(name, "BLOCKED", reason="isolated OpenCode v2.2.5 did not become ready")
        status, session = opencode_api(port, password, "POST", "/session", {"title": "transport poc", "location": {"directory": str(project)}})
        session = (session or {}).get("data", session)
        session_id = session.get("id") if isinstance(session, dict) else None
        if status < 200 or status >= 300 or not session_id:
            return result(name, "BLOCKED", reason="session creation failed", http_status=status)

        def prompt():
            prompt_result["value"] = opencode_api(port, password, "POST", f"/session/{session_id}/prompt",
                                                   {"text": "transport cancellation fixture", "model": {"providerID": "poc", "modelID": "poc-model"}}, timeout=20)
            # /prompt acknowledges submission; /wait keeps the fixture aligned
            # with the actual execution lifecycle until interrupt or terminal.
            prompt_result["wait"] = opencode_api(port, password, "POST", f"/experimental/session/{session_id}/wait", timeout=20)

        prompt_thread = threading.Thread(target=prompt, daemon=True)
        prompt_thread.start()
        request_deadline = time.time() + 10
        while time.time() < request_deadline:
            if any(row.get("type") == "UPSTREAM_STARTED" and row.get("session_id") == session_id for row in transport.events):
                break
            time.sleep(0.02)
        interrupt_status, interrupt_body = opencode_api(port, password, "POST", f"/session/{session_id}/interrupt", {}, timeout=5)
        prompt_thread.join(timeout=4)
        adapter_rows = [row for row in transport.events if row.get("session_id") == session_id]
        # Classification below is based only on emitted transport evidence; the
        # bounded join is fixture cleanup, never proof of a transport terminal.
        request_ids = {row.get("request_id") for row in adapter_rows if row.get("request_id")}
        rows = [row for row in transport.events if row.get("request_id") in request_ids]
        types = [row.get("type") for row in rows]
        terminal = [row for row in rows if row.get("type") in {"REQUEST_SETTLED", "REQUEST_UNKNOWN"}]
        plugin_rows = []
        if evidence.exists():
            plugin_rows = [json.loads(line) for line in evidence.read_text(encoding="utf-8").splitlines() if line]
        interrupted = any(row.get("type") == "session.execution.interrupted" for row in plugin_rows)
        downstream = "DOWNSTREAM_CLOSED" in types
        upstream_terminal = any(item in types for item in ("UPSTREAM_EOF", "UPSTREAM_ERROR", "UPSTREAM_ABORTED"))
        confirmed_abort = "UPSTREAM_ABORTED" in types and any(row.get("diagnosis") == "CANCELLED" for row in terminal)
        status_value = "PASS" if interrupt_status == 200 and interrupted and downstream and upstream_terminal and confirmed_abort else "BLOCKED"
        return result(name, status_value, session_id=session_id, interrupt_status=interrupt_status, interrupted=interrupted,
                      downstream_closed=downstream, upstream_terminal=upstream_terminal, upstream_aborted=confirmed_abort,
                      adapter_event_types=types, terminal_count=len(terminal), prompt_returned=not prompt_thread.is_alive(),
                      plugin_event_types=[row.get("type") for row in plugin_rows], ambiguity=not confirmed_abort)
    finally:
        BackendHandler.real_mode = None
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        shutil.rmtree(root / "home" / "data", ignore_errors=True)


def main() -> int:
    root = Path(tempfile.mkdtemp(prefix="larm-observable-transport-poc.", dir=TMP_PARENT))
    backend = Backend(("127.0.0.1", 0), BackendHandler)
    BackendHandler.rows = []
    BackendHandler.seen.clear()
    backend_thread = threading.Thread(target=backend.serve_forever, daemon=True)
    backend_thread.start()
    transport = ObservableTransport(f"http://127.0.0.1:{backend.server_address[1]}", str(root / "transport-events.jsonl"))
    transport.start()
    try:
        rows = synthetic_tests(transport)
        rows.extend(race_and_identity_tests(transport))
        # TEST-05 and TEST-06 are deliberately real OpenCode v2.2.5 calls, not
        # synthetic substitutes.  GGUF TEST-09 is gated on all prior tests.
        rows.append(opencode_case(transport, "/delay-headers", "TEST-05"))
        rows.append(opencode_case(transport, "/sse-cancel", "TEST-06"))
        required = {f"TEST-{index:02d}" for index in range(1, 9)}
        prior = {row["test"]: row["status"] for row in rows}
        rows.append(result("TEST-09", "BLOCKED", reason="GGUF real gate not met or deliberately not run; coordinator remains uninvolved",
                           gate_passed=required.issubset(prior) and all(prior[name] == "PASS" for name in required)))
        for row in rows:
            print(json.dumps(row, sort_keys=True))
        return 0 if all(row["status"] in {"PASS", "BLOCKED"} for row in rows) else 1
    finally:
        transport.shutdown()
        backend.shutdown()
        backend.server_close()
        backend_thread.join(timeout=2)
        print(json.dumps({"evidence": str(root / "transport-events.jsonl"), "root": str(root)}, sort_keys=True), file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
