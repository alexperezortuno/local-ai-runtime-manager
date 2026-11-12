#!/usr/bin/env python3
"""Exercise OpenCode through LARM's authoritative OpenAI-compatible proxy."""

import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
from hashlib import sha256
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
TMP = Path("/private/var/folders/zr/hnnn53p16qb_vh8tl5c73k2c0000gn/T")
DEFAULT_MODEL = Path("/Users/odin/AI/runtime/models/Qwen2.5-Coder-14B-Instruct-Q5_K_M.gguf")


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def api(url, method="GET", body=None, timeout=10, headers=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers={
        **({"Content-Type": "application/json"} if data else {}), **(headers or {})})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
        try:
            return response.status, json.loads(raw) if raw else None
        except json.JSONDecodeError:
            return response.status, raw.decode(errors="replace")


def main():
    model = Path(os.environ.get("LARM_REAL_GGUF", str(DEFAULT_MODEL))).expanduser()
    llama = shutil.which("llama-server")
    opencode = shutil.which("opencode") or "/Users/odin/.opencode/bin/opencode"
    if not llama or not Path(llama).is_file() or not model.is_file():
        print(json.dumps({"result": "BLOCKED", "reason": "local llama-server or GGUF unavailable",
                          "binary": llama, "model": str(model)}))
        return 2

    root = Path(tempfile.mkdtemp(prefix="larm-006.6-real-gguf.", dir=TMP))
    project = root / "project"
    project.mkdir()
    (project / "lease-check.txt").write_text("LARM-REAL-GGUF-ADAPTER-7e4a3d\n", encoding="utf-8")
    evidence = root / "plugin-events.jsonl"
    launcher_evidence = root / "launcher-events.jsonl"
    larm_log = root / "larm.log"
    opencode_log = root / "opencode.log"
    larm_port, model_port, opencode_port = port(), port(), port()
    larm_url = f"http://127.0.0.1:{larm_port}"
    server_url = f"http://127.0.0.1:{opencode_port}"
    config = root / "larm.yaml"
    config.write_text(f'''server:\n  host: 127.0.0.1\n  port: {larm_port}\nruntime:\n  backend: llamacpp\n  binary: {llama}\n  startup_timeout: 300s\n  shutdown_timeout: 30s\nscheduler:\n  max_active_models: 1\n  max_queue_size: 8\n  lease_ttl: 1s\n  heartbeat_interval: 200ms\n  max_lease_duration: 10m\nproxy:\n  queue_wait_timeout: 5m\n  inference_timeout: 10m\n  backend_connect_timeout: 5s\n  stream_idle_timeout: 2m\nmodels:\n  code-agent:\n    path: {model}\n    port: {model_port}\n    context: 8192\n    batch: 512\n    ubatch: 128\n    gpu_layers: all\n    cache_type_k: q8_0\n    cache_type_v: q8_0\n    parallel: 1\n    temperature: 0.15\n    top_p: 0.85\n    top_k: 40\n    reasoning: off\n    extra_args: ["--jinja", "--props"]\n''', encoding="utf-8")

    processes = []
    try:
        llama_version = subprocess.run([llama, "--version"], capture_output=True, text=True, check=True)
        larm_bin = root / "larm"
        subprocess.run(["go", "build", "-o", str(larm_bin), "./cmd/larm"], cwd=REPO, check=True)
        larm_process = subprocess.Popen([str(larm_bin), "-config", str(config)], stdout=subprocess.DEVNULL,
                                        stderr=larm_log.open("w"))
        processes.append(larm_process)

        opencode_dir = project / ".opencode"
        plugin_dir = opencode_dir / "plugins"
        plugin_dir.mkdir(parents=True)
        shutil.copy2(REPO / "integrations/opencode/larm-gate.ts", plugin_dir / "larm-gate.ts")
        (opencode_dir / "opencode.json").write_text(json.dumps({
            "model": "larm/code-agent",
            "permission": {"read": "allow", "grep": "allow"},
            "providers": {"larm": {"name": "LARM OpenAI-compatible proxy",
                "package": "@opencode/ai/providers/openai-compatible",
                "settings": {"baseURL": f"{larm_url}/v1", "headerTimeout": 600000},
                "models": {"code-agent": {"modelID": "code-agent", "name": "Local GGUF",
                    "tool_call": True, "body": {"tool_choice": "required"},
                    "capabilities": {"tools": True, "input": ["text"], "output": ["text"]},
                    "limit": {"context": 8192, "output": 512}}}}},
        }), encoding="utf-8")
        env = os.environ.copy()
        for name, directory in (("HOME", "home"), ("XDG_CONFIG_HOME", "home/config"),
                                ("XDG_DATA_HOME", "home/data"), ("XDG_CACHE_HOME", "home/cache"),
                                ("XDG_STATE_HOME", "home/state")):
            value = root / directory
            value.mkdir(parents=True, exist_ok=True)
            env[name] = str(value)
        password = uuid.uuid4().hex
        auth = "Basic " + base64.b64encode(f"opencode:{password}".encode()).decode()
        env.update({"OPENCODE_CONFIG": str(opencode_dir / "opencode.json"), "OPENCODE_DISABLE_AUTOUPDATE": "1",
                    "OPENCODE_SERVER_PASSWORD": password, "OPENCODE_SERVER_USERNAME": "opencode",
                    "LARM_OPENCODE_PROVIDER_ID": "larm",
                    "LARM_OPENCODE_EVIDENCE": str(evidence), "LARM_OPENCODE_LAUNCHER_EVIDENCE": str(launcher_evidence)})
        opencode_process = subprocess.Popen([opencode, "serve", "--hostname", "127.0.0.1", "--port", str(opencode_port)],
                                            cwd=project, env=env, stdout=subprocess.DEVNULL,
                                            stderr=opencode_log.open("w"))
        processes.append(opencode_process)

        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            try:
                api(f"{larm_url}/health", timeout=1)
                _, info = api(f"{server_url}/api/info", headers={"Authorization": auth}, timeout=1)
                info = info.get("data", info) if isinstance(info, dict) else {}
                if str(info.get("version", "")).endswith("2.2.5"):
                    break
            except Exception:
                if larm_process.poll() is not None or opencode_process.poll() is not None:
                    raise RuntimeError("isolated LARM/OpenCode service exited before readiness")
            time.sleep(0.2)
        else:
            raise RuntimeError("isolated LARM/OpenCode services failed readiness")

        _, providers = api(f"{server_url}/api/provider", headers={"Authorization": auth})
        providers = providers.get("data", providers) if isinstance(providers, dict) else providers
        provider_rows = providers if isinstance(providers, list) else providers.get("all", [])
        provider = next(row for row in provider_rows if row.get("id") == "larm")
        effective_model = provider.get("models", {}).get("code-agent", {})
        status, session = api(f"{server_url}/api/session", "POST", {
            "title": "LARM OpenCode GGUF adapter test", "location": {"directory": str(project)}},
            headers={"Authorization": auth})
        if status < 200 or status >= 300:
            raise RuntimeError(f"session creation failed: HTTP {status}")
        session_id = (session.get("data", session)).get("id")
        launcher = [sys.executable, str(REPO / "integrations/opencode/launcher.py"), "--server", server_url,
                    "--provider-id", "larm", "--model", "code-agent", "--directory", str(project),
                    "--session", session_id, "--timeout", "900"]
        prompt = ("Use the native read tool exactly once to read lease-check.txt; do not use any other tool. "
                  "Then return only the exact line from that file.")
        result = subprocess.run(launcher, input=prompt, text=True, capture_output=True, timeout=1100, env=env)
        try:
            launcher_result = json.loads(result.stdout.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError):
            launcher_result = {}
        if not launcher_result.get("execution_id"):
            raise RuntimeError(f"launcher produced no execution metadata: {result.stderr[-500:]}")
        events = [json.loads(line) for line in evidence.read_text().splitlines() if line] if evidence.exists() else []
        launcher_events = [json.loads(line) for line in launcher_evidence.read_text().splitlines() if line] if launcher_evidence.exists() else []
        _, inferences = api(f"{larm_url}/api/v1/inferences")
        inferences = inferences if isinstance(inferences, list) else inferences.get("data", [])
        larm_inferences = [row for row in inferences if row.get("execution_id") == launcher_result["execution_id"]]
        _, messages = api(f"{server_url}/api/session/{session_id}/message", headers={"Authorization": auth})
        messages = messages.get("data", messages) if isinstance(messages, dict) else messages
        tool_calls = [{"tool_call_id": part.get("callID"), "tool_name": part.get("tool"),
                       "tool_state": part.get("state", {}).get("status")}
                      for message in messages if isinstance(message, dict)
                      for part in message.get("parts", [])
                      if isinstance(part, dict) and part.get("type") == "tool"]
        try:
            _, properties = api(f"http://127.0.0.1:{model_port}/props", timeout=5)
        except Exception:
            properties = {}
        template = properties.get("chat_template") if isinstance(properties, dict) else None
        status = api(f"{larm_url}/api/v1/status")[1]
        output = {"result": "PASS" if result.returncode == 0 and larm_inferences and
                  all(row.get("transport_state") in {"COMPLETED", "BODY_COMPLETED"} for row in larm_inferences) else "BLOCKED",
                  "execution_id": launcher_result.get("execution_id"), "session_id": session_id,
                  "model_id": "code-agent", "request_ids": [row.get("request_id") for row in larm_inferences],
                  "request_count": len(larm_inferences), "larm_inferences": larm_inferences,
                  "effective_model_capabilities": effective_model, "opencode_tool_calls": tool_calls,
                  "adapter_events": [row for row in events if row.get("execution_id") == launcher_result["execution_id"]],
                  "launcher_events": launcher_events, "runtime_state": status.get("state"),
                  "llama_server_version": (llama_version.stdout or llama_version.stderr).strip(),
                  "gguf_model": {"path": str(model), "size_bytes": model.stat().st_size},
                  "chat_template": {"present": isinstance(template, str),
                                    "sha256": sha256(template.encode()).hexdigest() if isinstance(template, str) else None,
                                    "bytes": len(template.encode()) if isinstance(template, str) else None},
                  "isolated_root": str(root), "launcher_exit_code": result.returncode}
        (root / "result.json").write_text(json.dumps(output, indent=2), encoding="utf-8")
        print(json.dumps(output, indent=2))
        return 0 if output["result"] == "PASS" else 2
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        shutil.rmtree(root / "home" / "data", ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
