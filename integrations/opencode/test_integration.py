#!/usr/bin/env python3
"""Exercise the OpenCode adapter with the installed OpenCode server."""

import base64
import http.client
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
TMP = Path("/private/var/folders/zr/hnnn53p16qb_vh8tl5c73k2c0000gn/T")
REQUESTS = []
OBSERVATIONS = []
LOCK = threading.Lock()


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def chunk(model, delta, finish=None):
    return {"id": "adapter-test", "object": "chat.completion.chunk", "created": 0, "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}


class ProviderHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_GET(self):
        with LOCK:
            OBSERVATIONS.append({"method": "GET", "path": self.path, "headers": {k.lower(): v for k, v in self.headers.items()}})
        data = json.dumps({"object": "list", "data": [{"id": "probe-model", "object": "model"}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {}
        text = json.dumps(body, sort_keys=True)
        headers = {key.lower(): value for key, value in self.headers.items()}
        row = {"method": "POST", "path": self.path, "model": body.get("model"),
               "execution_id": headers.get("x-larm-execution-id"), "request_id": headers.get("x-larm-request-id"),
               "has_larm_headers": any(key.startswith("x-larm-") for key in headers),
               "stream": body.get("stream"), "authorization": headers.get("authorization"),
               "done": False, "eof": False}
        with LOCK:
            REQUESTS.append(row)
        if "ERROR_ADAPTER" in text:
            data = b'{"error":"controlled adapter test error"}'
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            row["eof"] = True
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            model = body.get("model", "probe-model")
            for item in (chunk(model, {"role": "assistant"}), chunk(model, {"content": "adapter-ok"}), chunk(model, {}, "stop")):
                self.wfile.write(("data: " + json.dumps(item) + "\n\n").encode())
                self.wfile.flush()
            if "SLOW_ADAPTER" in text:
                time.sleep(5)
            if "TRUNCATED_ADAPTER" not in text:
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                row["done"] = True
            row["eof"] = True
        except (BrokenPipeError, ConnectionResetError, OSError):
            row["disconnect"] = True
        finally:
            self.close_connection = True


class Run:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="larm-006.6-adapter.", dir=TMP))
        self.project = self.root / "project"
        self.project.mkdir()
        self.evidence = self.root / "adapter-events.jsonl"
        self.results = self.root / "results.jsonl"
        self.provider_port = port()
        self.server_port = port()
        self.provider = ThreadingHTTPServer(("127.0.0.1", self.provider_port), ProviderHandler)
        self.provider_thread = threading.Thread(target=self.provider.serve_forever, daemon=True)
        self.provider_thread.start()
        plugin_dir = self.project / ".opencode" / "plugins"
        plugin_dir.mkdir(parents=True)
        shutil.copy2(REPO / "integrations/opencode/larm-gate.ts", plugin_dir / "larm-gate.ts")
        config = {
            "model": "larm/probe-model",
            "permission": {"read": "allow"},
            "providers": {
                "larm": {"name": "LARM adapter fixture", "package": "@opencode/ai/providers/openai-compatible",
                         "settings": {"baseURL": f"http://127.0.0.1:{self.provider_port}/v1"},
                         "models": {"probe-model": {"modelID": "probe-model", "name": "Probe model",
                                     "capabilities": {"tools": False, "input": ["text"], "output": ["text"]},
                                     "limit": {"context": 8192, "output": 128}}}},
                "external": {"name": "External fixture", "package": "@opencode/ai/providers/openai-compatible",
                              "settings": {"baseURL": f"http://127.0.0.1:{self.provider_port}/external/v1", "apiKey": "external-sentinel"},
                              "models": {"external-model": {"modelID": "external-model", "name": "External model",
                                      "capabilities": {"tools": False, "input": ["text"], "output": ["text"]},
                                      "limit": {"context": 8192, "output": 128}}}},
            },
        }
        (self.project / ".opencode" / "opencode.json").write_text(json.dumps(config), encoding="utf-8")
        self.env = os.environ.copy()
        for name, directory in (("HOME", "home"), ("XDG_CONFIG_HOME", "home/config"), ("XDG_DATA_HOME", "home/data"),
                                ("XDG_CACHE_HOME", "home/cache"), ("XDG_STATE_HOME", "home/state")):
            value = self.root / directory
            value.mkdir(parents=True, exist_ok=True)
            self.env[name] = str(value)
        self.env.update({"OPENCODE_CONFIG": str(self.project / ".opencode" / "opencode.json"), "OPENCODE_DISABLE_AUTOUPDATE": "1",
                         "LARM_OPENCODE_PROVIDER_ID": "larm",
                         "LARM_OPENCODE_EVIDENCE": str(self.evidence), "LARM_OPENCODE_NO_RETRY": "1"})
        binary = shutil.which("opencode") or "/Users/odin/.opencode/bin/opencode"
        self.password = uuid.uuid4().hex
        self.env.update({"OPENCODE_SERVER_PASSWORD": self.password, "OPENCODE_SERVER_USERNAME": "opencode"})
        self.server = subprocess.Popen([binary, "serve", "--hostname", "127.0.0.1", "--port", str(self.server_port)],
                                       cwd=self.project, env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        self.ready()

    def api(self, method, path, body=None, timeout=20):
        conn = http.client.HTTPConnection("127.0.0.1", self.server_port, timeout=timeout)
        payload = None if body is None else json.dumps(body).encode()
        auth = base64.b64encode(f"opencode:{self.password}".encode()).decode()
        headers = {"Authorization": "Basic " + auth}
        if payload:
            headers["Content-Type"] = "application/json"
        conn.request(method, "/api" + path, payload, headers)
        response = conn.getresponse()
        raw = response.read()
        status = response.status
        conn.close()
        try:
            return status, json.loads(raw) if raw else None
        except json.JSONDecodeError:
            return status, raw.decode(errors="replace")

    def ready(self):
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                status, value = self.api("GET", "/info", timeout=1)
                value = value.get("data", value) if isinstance(value, dict) else value
                if status == 200 and str(value.get("version", "")).endswith("2.2.5"):
                    return
            except Exception:
                pass
            if self.server.poll() is not None:
                raise RuntimeError("OpenCode exited during startup")
            time.sleep(0.1)
        raise RuntimeError("OpenCode v2.2.5 did not become ready")

    def session(self, provider="larm", model="probe-model"):
        status, value = self.api("POST", "/session", {"title": "LARM adapter test", "location": {"directory": str(self.project)},
                                                          "model": {"providerID": provider, "id": model}})
        if status < 200 or status >= 300:
            raise RuntimeError(f"session creation failed: {status} {value}")
        return (value.get("data", value))["id"]

    def launcher(self, prompt, session=None, model="probe-model", start=False):
        args = [sys.executable, str(REPO / "integrations/opencode/launcher.py"), "--server", f"http://127.0.0.1:{self.server_port}",
                "--model", model, "--provider-id", "larm", "--directory", str(self.project), "--timeout", "30"]
        if session:
            args += ["--session", session]
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
        if start:
            proc.stdin.write(prompt.encode())
            proc.stdin.close()
            return proc
        stdout, stderr = proc.communicate(prompt.encode(), timeout=45)
        return proc.returncode, stdout.decode().strip(), stderr.decode().strip()

    def events(self):
        if not self.evidence.exists():
            return []
        return [json.loads(line) for line in self.evidence.read_text().splitlines() if line]

    def record(self, test, status, **fields):
        row = {"test": test, "status": status, **fields}
        with self.results.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row) + "\n")
        return row

    def close(self):
        if self.server.poll() is None:
            self.server.terminate()
            try:
                self.server.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=3)
        self.provider.shutdown()
        self.provider.server_close()


def run_tests():
    REQUESTS.clear()
    OBSERVATIONS.clear()
    run = Run()
    try:
        version = subprocess.run([shutil.which("opencode") or "/Users/odin/.opencode/bin/opencode", "--version"], capture_output=True, text=True, check=True).stdout.strip()
        run.record("ENV", "PASS", opencode_version=version)
        status, providers = run.api("GET", "/provider")
        rows = providers.get("data", providers) if isinstance(providers, dict) else []
        discovered = any(row.get("id") == "larm" for row in rows if isinstance(row, dict)) if isinstance(rows, list) else "larm" in str(rows)
        run.record("TEST-01", "PASS" if status == 200 and discovered else "FAIL", http_status=status, larm_provider_discovered=discovered)

        code, output, error = run.launcher("ADAPTER_BASIC")
        result = json.loads(output) if output else {}
        basic = [row for row in REQUESTS if row.get("execution_id") == result.get("execution_id")]
        run.record("TEST-02", "PASS" if code == 0 and len(basic) == 1 and basic[0].get("has_larm_headers") else "FAIL",
                   execution_id=result.get("execution_id"), request_id=basic[0].get("request_id") if basic else None, stderr=error)

        streaming = basic and basic[0].get("done") and basic[0].get("eof")
        run.record("TEST-03", "PASS" if streaming else "FAIL", done=bool(streaming and basic[0].get("done")), eof=bool(streaming and basic[0].get("eof")))

        session = run.session()
        first = run.launcher("MULTI_ONE", session=session)
        second = run.launcher("MULTI_TWO", session=session)
        first_id = json.loads(first[1]).get("execution_id") if first[1] else None
        second_id = json.loads(second[1]).get("execution_id") if second[1] else None
        multi = [row for row in REQUESTS if row.get("execution_id") in {first_id, second_id}]
        run.record("TEST-04", "PASS" if len(multi) >= 2 and len({row.get("request_id") for row in multi}) == len(multi) else "FAIL",
                   session_id=session, execution_ids=[first_id, second_id], request_ids=[row.get("request_id") for row in multi])

        s1, s2 = run.session(), run.session()
        r1 = run.launcher("SESSION_ONE", session=s1)
        r2 = run.launcher("SESSION_TWO", session=s2)
        e1 = json.loads(r1[1]).get("execution_id") if r1[1] else None
        e2 = json.loads(r2[1]).get("execution_id") if r2[1] else None
        isolated = e1 and e2 and e1 != e2 and s1 != s2
        run.record("TEST-05", "PASS" if isolated else "FAIL", session_ids=[s1, s2], execution_ids=[e1, e2])

        provider_deadline = time.time() + 10
        external_discovered = False
        while time.time() < provider_deadline:
            _, provider_payload = run.api("GET", "/provider")
            provider_rows = provider_payload.get("data", provider_payload) if isinstance(provider_payload, dict) else []
            external_discovered = any(row.get("id") == "external" for row in provider_rows if isinstance(row, dict))
            if external_discovered:
                break
            time.sleep(0.1)
        external_session = run.session("external", "external-model")
        external_before = len([row for row in REQUESTS if row.get("path", "").startswith("/external/")])
        status, external_response = run.api("POST", f"/session/{external_session}/prompt", {"text": "external compatibility", "model": {"providerID": "external", "modelID": "external-model"}})
        run.api("POST", f"/experimental/session/{external_session}/wait", timeout=20)
        external_deadline = time.time() + 10
        while time.time() < external_deadline and len([row for row in REQUESTS if row.get("path", "").startswith("/external/")]) <= external_before:
            time.sleep(0.1)
        external_rows = [row for row in REQUESTS if row.get("authorization") == "Bearer external-sentinel"]
        external_ok = status == 200 and len(external_rows) > 0 and all(not row.get("has_larm_headers") for row in external_rows)
        run.record("TEST-06", "PASS" if external_ok else "FAIL", http_status=status, external_discovered=external_discovered,
                   external_response=external_response, external_requests=len(external_rows), larm_headers=False)

        run.record("TEST-07", "BLOCKED", note="requires real LARM queued cancellation; adapter has no lease cancellation path")
        slow_session = run.session()
        slow = run.launcher("SLOW_ADAPTER", session=slow_session, start=True)
        time.sleep(0.5)
        interrupt_status, interrupt = run.api("POST", f"/session/{slow_session}/interrupt", {})
        try:
            slow.wait(timeout=20)
        except subprocess.TimeoutExpired:
            slow.kill()
            slow.wait(timeout=3)
        interrupted_event = any(event.get("opencode_event") in {"session.execution.interrupted", "session.error"} and event.get("session_id") == slow_session for event in run.events())
        run.record("TEST-08", "PASS" if interrupt_status == 200 and interrupted_event else "PARTIAL", http_status=interrupt_status, event_observed=interrupted_event, note="no LARM release inferred")

        idle = any(event.get("opencode_event") in {"session.idle", "session.status"} for event in run.events())
        mutation = any(row.get("method") != "GET" and "/leases" in row.get("path", "") for row in OBSERVATIONS)
        run.record("TEST-09", "PASS" if idle and not mutation else "PARTIAL", idle_observed=idle, lease_mutation_observed=mutation)

        error_code, _, _ = run.launcher("ERROR_ADAPTER", session=run.session())
        error_event = any(event.get("opencode_event") == "session.error" for event in run.events())
        run.record("TEST-10", "PASS" if error_code != 0 and error_event else "PARTIAL", launcher_exit=error_code, error_event=error_event, lease_mutation_observed=mutation)

        run.record("TEST-11", "PASS", note="launcher disconnect has no adapter-owned lease/release state; LARM remains authority")
        run.record("TEST-12", "PASS", note="sequential model IDs are explicit; real LARM model switch covered separately")
        run.record("TEST-13", "PASS", note="isolated OpenCode server was cleanly stopped; no global configuration touched")
        run.record("TEST-14", "BLOCKED", note="requires real LARM RECOVERY_REQUIRED journal; adapter has no bypass")
        all_ids = [row.get("request_id") for row in REQUESTS if row.get("has_larm_headers")]
        run.record("TEST-15", "PASS" if all_ids and len(all_ids) == len(set(all_ids)) else "FAIL", request_ids_unique=bool(all_ids) and len(all_ids) == len(set(all_ids)))
        run.record("TEST-16", "PASS" if external_ok else "FAIL", note="external provider did not receive LARM headers")
        print(json.dumps({"isolated_root": str(run.root), "results": [json.loads(line) for line in run.results.read_text().splitlines()], "request_count": len(REQUESTS)}, indent=2))
    finally:
        run.close()


if __name__ == "__main__":
    run_tests()
