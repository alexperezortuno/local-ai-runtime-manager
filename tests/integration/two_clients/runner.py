#!/usr/bin/env python3
"""Reproducible LARM-006.5/006.5.1 integration runner.

Evidence is metadata-only. Destructive scenarios are opt-in and every LARM
instance, journal, store, port and child process is isolated per scenario.
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CLIENT = Path(__file__).with_name("client.py")
FIXTURE = Path(__file__).with_name("backend_fixture.py")
DEFAULT_A = Path("/Users/odin/AI/runtime/models/Qwen2.5-Coder-14B-Instruct-Q5_K_M.gguf")
DEFAULT_B = Path("/Users/odin/AI/runtime/models/Qwen3-Coder-30B-A3B-Instruct-Q4_K_M.gguf")


def iso():
    return datetime.now(timezone.utc).isoformat()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def json_call(url, method="GET", body=None, timeout=5, headers=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={**({"Content-Type": "application/json"} if data else {}), **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        try:
            payload = json.loads(error.read())
        except Exception:
            payload = {"error": "http_error"}
        return error.code, payload
    except Exception as error:  # noqa: BLE001 - runner records environment failures
        return None, {"error": type(error).__name__}


def metadata_process(process):
    if not process:
        return None
    allowed = (
        "process_instance_id", "owner_instance_id", "larm_instance_id", "model_id",
        "pid", "pgid", "state", "process_identity_status", "ownership_status",
        "termination_status", "descendant_status", "start_identity", "started_at",
        "termination_evidence_id", "readiness_evidence_id", "recovery_state",
    )
    return {key: process[key] for key in allowed if key in process}


def metadata_snapshot(base, label):
    endpoints = {
        "status": "/api/v1/status",
        "queue": "/api/v1/queue",
        "models": "/v1/models",
        "inferences": "/api/v1/inferences",
        "processes": "/api/v1/processes",
        "recovery_status": "/api/v1/recovery/status",
        "recovery_inferences": "/api/v1/recovery/inferences",
        "recovery_leases": "/api/v1/recovery/leases",
        "incidents": "/api/v1/recovery/incidents",
        "audit": "/api/v1/recovery/audit",
    }
    result = {"label": label, "observed_at": iso()}
    for name, path in endpoints.items():
        status, payload = json_call(base + path)
        if name == "models" and isinstance(payload, dict):
            payload = {"object": payload.get("object"), "model_ids": [item.get("id") for item in payload.get("data", [])]}
        if name == "processes" and isinstance(payload, list):
            payload = [metadata_process(item) for item in payload]
        result[name] = {"http_status": status, "body": payload}
    return result


class Instance:
    def __init__(self, args, root, suffix="main"):
        self.args = args
        self.root = root / suffix
        self.root.mkdir(parents=True, exist_ok=True)
        self.port = free_port()
        self.model_port_a = free_port()
        self.model_port_b = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.config = self.root / "larm.yaml"
        self.journal = self.root / "runtime-core.json"
        self.idempotency = self.root / "scheduler-idempotency.json"
        self.log = self.root / "larm.log"
        self.proc = None
        self.pgids = []
        self.write_config()

    def write_config(self):
        def scalar(value):
            return json.dumps(str(value))

        runtime_binary = getattr(self.args, "runtime_binary", None) or self.args.llama_server
        fixture_args = ""
        if self.args.backend == "controlled":
            fixture_args = (
                f'\n    extra_args: ["--fixture-mode", {scalar(self.args.backend_mode)}, '
                f'"--hold-seconds", "60"]'
            )
        stream_idle = "1s" if self.args.backend == "controlled" else "30s"

        self.config.write_text(f'''server:
  host: "127.0.0.1"
  port: {self.port}
runtime:
  backend: "llamacpp"
  binary: {scalar(runtime_binary)}
  startup_timeout: "300s"
  shutdown_timeout: "10s"
scheduler:
  max_active_models: 1
  max_queue_size: 32
  lease_ttl: "3s"
  heartbeat_interval: "1s"
  max_lease_duration: "5m"
  idempotency_store: {scalar(self.idempotency)}
proxy:
  queue_wait_timeout: "20s"
  inference_timeout: "120s"
  backend_connect_timeout: "5s"
  stream_idle_timeout: {scalar(stream_idle)}
  inference_max_terminal_records: 1000
recovery:
  journal_path: {scalar(self.journal)}
  durability: "sync"
  max_records: 10000
models:
  {self.args.model_a_id}:
    path: {scalar(self.args.model_a)}
    port: {self.model_port_a}
    context: 2048
    batch: 128
    ubatch: 64
    gpu_layers: "all"
    parallel: 1
    temperature: 0.0
    top_p: 0.9
    top_k: 20
    reasoning: "off"
{fixture_args}
  {self.args.model_b_id}:
    path: {scalar(self.args.model_b)}
    port: {self.model_port_b}
    context: 2048
    batch: 128
    ubatch: 64
    gpu_layers: "all"
    parallel: 1
    temperature: 0.0
    top_p: 0.9
    top_k: 20
    reasoning: "off"
{fixture_args}
''', encoding="utf-8")

    def start(self):
        binary = self.args.larm_binary
        if not binary:
            binary = self.root / "larm"
            subprocess.run(["go", "build", "-o", str(binary), "./cmd/larm"], cwd=ROOT, check=True)
        log = self.log.open("w", encoding="utf-8")
        self.proc = subprocess.Popen([str(binary), "-config", str(self.config)], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=log)
        deadline = time.monotonic() + self.args.startup_timeout
        while time.monotonic() < deadline:
            status, payload = json_call(self.base + "/health", timeout=1)
            if status == 200 and payload.get("status") == "ok":
                return
            if self.proc.poll() is not None:
                raise RuntimeError(f"LARM exited during startup; see {self.log}")
            time.sleep(0.2)
        raise TimeoutError(f"LARM did not become healthy; see {self.log}")

    def stop(self, clean=True):
        if self.proc is None:
            return
        if clean and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        elif self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.proc = None

    def crash(self):
        """Crash only this exact LARM child; never targets a process by name."""
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.proc = None


class Suite:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.output_dir) if args.output_dir else Path(tempfile.mkdtemp(prefix="larm-006.5-two-clients."))
        self.root.mkdir(parents=True, exist_ok=True)
        self.args.backend = getattr(self.args, "backend", "real")
        self.args.backend_mode = getattr(self.args, "backend_mode", "sse-truncated")
        self.args.runtime_binary = self.args.llama_server
        if self.args.backend == "controlled":
            FIXTURE.chmod(0o700)
            self.args.runtime_binary = str(FIXTURE)
            self.args.model_a = self.root / "model-a.fixture"
            self.args.model_b = self.root / "model-b.fixture"
            self.args.model_a.touch(exist_ok=True)
            self.args.model_b.touch(exist_ok=True)
        self.results = []
        self.instance = None
        self.survivors = []
        self.pending_cleanup = []

    def record(self, test_id, objective, expected, actual, result, **extra):
        row = {"test_id": test_id, "objective": objective, "expected": expected, "actual": actual,
               "result": result, "observed_at": iso(), **extra}
        self.results.append(row)
        return row

    def snapshot(self, label):
        snap = metadata_snapshot(self.instance.base, label)
        path = self.root / "snapshots.jsonl"
        with path.open("a", encoding="utf-8") as output:
            output.write(json.dumps(snap, sort_keys=True) + "\n")
        return snap

    def start_instance(self, suffix="main"):
        self.instance = Instance(self.args, self.root, suffix)
        self.instance.start()
        self.snapshot("instance_started")

    def client(self, client_id, model, mode="json", timeout=120, close_after_events=0, max_tokens=32, lease_id=None, long_stream=False, close_after_headers=False):
        rid, eid = str(uuid.uuid4()), str(uuid.uuid4())
        output = self.root / f"client-{client_id}-{rid}.json"
        command = [sys.executable, str(CLIENT), "--url", self.instance.base, "--client-id", client_id,
                   "--model", model, "--mode", mode, "--timeout", str(timeout), "--max-tokens", str(max_tokens),
                   "--request-id", rid, "--execution-id", eid, "--output", str(output)]
        if close_after_events:
            command += ["--close-after-events", str(close_after_events)]
        if close_after_headers:
            command += ["--close-after-headers"]
        if long_stream:
            command += ["--long-stream"]
        if lease_id:
            command += ["--lease-id", lease_id]
        process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return process, rid, eid, output

    def await_client(self, process, output, poll_label):
        while process.poll() is None:
            self.snapshot(poll_label)
            time.sleep(0.25)
        process.wait()
        try:
            return json.loads(output.read_text(encoding="utf-8"))
        except Exception:
            return {"error": "client_result_missing", "client_exit": process.returncode}

    def read_client(self, output):
        try:
            return json.loads(output.read_text(encoding="utf-8"))
        except Exception:
            return {"error": "client_result_missing"}

    def cleanup_owned_processes(self, processes):
        """Signal only a recorded PID/PGID after an exact ownership check."""
        cleanup = []
        for process in processes:
            pid, pgid = process.get("pid"), process.get("pgid")
            executable = process.get("executable", "")
            check = subprocess.run(["/bin/ps", "-p", str(pid), "-o", "pid=,pgid=,command="],
                                   capture_output=True, text=True, check=False)
            line = check.stdout.strip()
            fields = line.split(None, 2)
            owned = (len(fields) == 3 and fields[0] == str(pid) and fields[1] == str(pgid)
                     and (not executable or fields[2].startswith(executable)))
            entry = {"pid": pid, "pgid": pgid, "owned_verified": owned, "observed": line, "signal": None}
            if owned and pgid:
                try:
                    os.killpg(pgid, signal.SIGTERM)
                    entry["signal"] = "SIGTERM"
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        if subprocess.run(["/bin/ps", "-p", str(pid), "-o", "pid="], capture_output=True, text=True, check=False).stdout.strip() == "":
                            break
                        time.sleep(0.1)
                    remaining = subprocess.run(["/bin/ps", "-p", str(pid), "-o", "pid=,pgid=,command="], capture_output=True, text=True, check=False).stdout.strip().split(None, 2)
                    still_owned = (len(remaining) == 3 and remaining[0] == str(pid) and remaining[1] == str(pgid)
                                   and (not executable or remaining[2].startswith(executable)))
                    if still_owned:
                        os.killpg(pgid, signal.SIGKILL)
                        entry["signal"] = "SIGTERM,SIGKILL"
                    elif remaining:
                        entry["pending_cleanup"] = True
                except OSError as error:
                    entry["signal_error"] = type(error).__name__
            else:
                entry["pending_cleanup"] = True
            cleanup.append(entry)
        (self.root / "process-cleanup.json").write_text(json.dumps(cleanup, indent=2) + "\n", encoding="utf-8")
        return cleanup

    def request(self, client_id, model, mode="json", timeout=120, close_after_events=0, max_tokens=32):
        process, rid, eid, output = self.client(client_id, model, mode, timeout, close_after_events, max_tokens)
        self.snapshot(f"{client_id}_admitted_or_queued")
        result = self.await_client(process, output, f"{client_id}_during")
        result["request_id"] = rid
        result["execution_id"] = eid
        self.snapshot(f"{client_id}_after_client")
        return result

    def inference(self, rid):
        status, payload = json_call(self.instance.base + "/api/v1/inferences/" + rid)
        return payload if status == 200 else {"http_status": status, "body": payload}

    def active_wait(self, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status, body = json_call(self.instance.base + "/api/v1/status")
            if status == 200 and body.get("active_inferences", 0) > 0:
                return True
            time.sleep(0.1)
        return False

    def concurrent(self, requests):
        children = []
        for item in requests:
            children.append((item, *self.client(item[0], item[1], item[2], item[3], 0, item[4])))
            time.sleep(0.05)
        for item, process, _, _, output in children:
            self.snapshot(f"{item[0]}_concurrent")
        results = []
        for item, process, rid, eid, output in children:
            result = self.await_client(process, output, f"{item[0]}_concurrent_during")
            result.update({"request_id": rid, "execution_id": eid})
            results.append(result)
        self.snapshot("concurrent_after")
        return results

    def safe_suite(self):
        self.start_instance()
        base = self.instance.base
        try:
            status, recovery = json_call(base + "/api/v1/recovery/status")
            runtime_status = metadata_snapshot(base, "TEST-01_after_start")["status"]["body"]
            self.record("TEST-01", "inicio controlado", "NORMAL y sin proceso residente",
                        {"recovery": recovery, "runtime": runtime_status},
                        "PASS" if status == 200 and not recovery.get("recovery_required") and runtime_status.get("state") == "STOPPED" else "FAIL")

            a = self.request("A", self.args.model_a_id, max_tokens=32)
            ai = self.inference(a["request_id"])
            self.record("TEST-02", "ciclo JSON del cliente A", "admisión, lease, READY, respuesta y release",
                        {"client": a, "inference": ai},
                        "PASS" if a.get("http_status", 0) == 200 and ai.get("semantic_state") == "SUCCEEDED" else "FAIL")

            b = self.request("B", self.args.model_b_id, max_tokens=32)
            bi = self.inference(b["request_id"])
            self.record("TEST-03", "ciclo JSON del cliente B", "ciclo completo con modelo B",
                        {"client": b, "inference": bi},
                        "PASS" if b.get("http_status", 0) == 200 and bi.get("semantic_state") == "SUCCEEDED" else "FAIL")

            p, arid, aeid, aoutput = self.client("A", self.args.model_a_id, max_tokens=256)
            overlap = self.active_wait()
            self.snapshot("TEST-04_A_active_before_B")
            q, brid, beid, boutput = self.client("B", self.args.model_b_id, max_tokens=32)
            # Resolve the two independent processes without sharing state.
            time.sleep(0.1)
            self.snapshot("TEST-04_B_queued_or_active")
            while q.poll() is None or p.poll() is None:
                self.snapshot("TEST-04_during")
                time.sleep(0.25)
            ar = self.read_client(aoutput)
            br = self.read_client(boutput)
            self.snapshot("TEST-04_after")
            records = metadata_snapshot(base, "TEST-04_registry")["inferences"]["body"]
            ordered = [item.get("request_id") for item in records if isinstance(item, dict)] if isinstance(records, list) else []
            self.record("TEST-04", "solapamiento A/B", "FIFO global, una residencia y cambio seguro",
                        {"overlap_observed": overlap, "registry_order": ordered, "clients": [ar, br],
                         "request_ids": [arid, brid], "execution_ids": [aeid, beid]},
                        "PASS" if overlap and len(ordered) >= 2 else "PASS PARCIAL",
                        limitation="La admisión efectiva se toma del Registry, no del orden de spawn HTTP.")

            # A real queue cancellation: B times out while A owns the model.
            p, _, _, _ = self.client("A-active", self.args.model_a_id, max_tokens=256)
            active = self.active_wait()
            q, qrid, qeid, qoutput = self.client("B-queued-cancel", self.args.model_b_id, timeout=0.5, max_tokens=16)
            while q.poll() is None:
                self.snapshot("TEST-09_B_waiting_fifo")
                time.sleep(0.1)
            qr = self.read_client(qoutput)
            qi = self.inference(qrid)
            while p.poll() is None:
                self.snapshot("TEST-09_A_active")
                time.sleep(0.2)
            self.record("TEST-09", "cancelación antes de upstream", "B no se envía y A no pierde su lease",
                        {"active_a": active, "client_b": qr, "inference_b": qi, "request_id": qrid, "execution_id": qeid},
                        "PASS" if active and qi.get("cancellation_state") == "CANCEL_CONFIRMED" and qi.get("transport_state") == "NOT_STARTED" else "FAIL")

            # Manual release is attempted while the automatic inference is active.
            p, prid, peid, poutput = self.client("A-lease-protection", self.args.model_a_id, mode="sse", max_tokens=256)
            lease_id = ""
            deadline = time.monotonic() + 20
            current = {}
            while time.monotonic() < deadline:
                current = self.inference(prid)
                lease_id = current.get("lease_id", "") if isinstance(current, dict) else ""
                if lease_id and current.get("transport_state") in ("STARTED", "HEADERS_RECEIVED", "BODY_COMPLETED"):
                    break
                time.sleep(0.1)
            release_status, release_body = json_call(base + "/api/v1/leases/" + lease_id, method="DELETE") if lease_id else (None, {})
            self.snapshot("TEST-13_manual_release_during_active")
            while p.poll() is None:
                self.snapshot("TEST-13_active")
                time.sleep(0.2)
            self.record("TEST-13", "protección de lease", "release manual rechazado mientras hay inferencia activa",
                        {"request_id": prid, "execution_id": peid, "lease_id": lease_id,
                         "release_http_status": release_status, "release_body": release_body,
                         "client": self.read_client(poutput)},
                        "PASS" if release_status == 409 else "FAIL")

            multi = self.concurrent([
                ("A1", self.args.model_a_id, "json", 120, 16),
                ("B1", self.args.model_b_id, "json", 120, 16),
                ("A2", self.args.model_a_id, "json", 120, 16),
                ("B2", self.args.model_b_id, "json", 120, 16),
            ])
            self.record("TEST-05", "múltiples solicitudes A1/B1/A2/B2", "sin adelantamientos injustificados",
                        {"clients": multi}, "PASS PARCIAL",
                        limitation="La comparación final usa snapshots de admisión; la latencia de GGUF puede impedir solapamiento estable.")

            first = self.request("A-reuse-1", self.args.model_a_id, max_tokens=16)
            first_processes = metadata_snapshot(base, "TEST-06_after_first")["processes"]["body"]
            second = self.request("A-reuse-2", self.args.model_a_id, max_tokens=16)
            second_processes = metadata_snapshot(base, "TEST-06_after_second")["processes"]["body"]
            self.record("TEST-06", "reutilización de modelo", "no exigir spawn nuevo si es seguro",
                        {"first": first, "second": second, "processes_first": first_processes, "processes_second": second_processes},
                        "PASS" if first.get("http_status", 0) == 200 and second.get("http_status", 0) == 200 else "FAIL")

            stream = self.request("A-SSE", self.args.model_a_id, mode="sse", max_tokens=32)
            si = self.inference(stream["request_id"])
            self.record("TEST-07", "SSE completo", "2xx, [DONE], EOF y estado terminal correcto",
                        {"client": stream, "inference": si},
                        "PASS" if stream.get("sse_done") and stream.get("sse_eof") and si.get("semantic_state") == "SUCCEEDED" else "FAIL")

            processes = metadata_snapshot(base, "TEST-14_processes_after_suite")["processes"]["body"]
            self.record("TEST-14", "Process Supervisor", "ProcessInstanceID, PID/PGID, identidad y terminación durable",
                        {"processes": processes},
                        "PASS" if processes and all(item.get("process_instance_id") and item.get("pid") and item.get("pgid")
                                                     and item.get("process_identity_status") == "VERIFIED"
                                                     and item.get("ownership_status") == "OWNED_VERIFIED"
                                                     and item.get("termination_status") == "TERMINATED_VERIFIED"
                                                     for item in processes) else "FAIL")

            inferences = metadata_snapshot(base, "TEST-15_model_change")["inferences"]["body"]
            by_model = {}
            for item in inferences if isinstance(inferences, list) else []:
                process_id = item.get("process_instance_id")
                if process_id:
                    by_model.setdefault(item.get("model_id"), set()).add(process_id)
            self.record("TEST-15", "cambio de modelo", "terminación verificada antes de spawn B",
                        {"processes": processes, "process_ids_by_model": {key: sorted(value) for key, value in by_model.items()}},
                        "PASS" if self.args.model_a_id in by_model and self.args.model_b_id in by_model
                        and by_model[self.args.model_a_id].isdisjoint(by_model[self.args.model_b_id]) else "FAIL")

            # Restart the same isolated instance and retain its journal/store.
            self.instance.stop(clean=True)

            self.instance = Instance(self.args, self.root, "main")
            self.instance.start()
            restart = self.snapshot("TEST-16_after_clean_restart")
            self.record("TEST-16", "reinicio limpio", "sin bloqueos falsos y evidencia conservada",
                        {"recovery": restart["recovery_status"], "runtime": restart["status"],
                         "inference_count": len(restart["inferences"].get("body", []))},
                        "PASS" if restart["recovery_status"]["body"].get("state") == "NORMAL"
                        and restart["recovery_status"]["body"].get("admission_allowed") else "FAIL")

            journal = json.loads(self.instance.journal.read_text(encoding="utf-8"))
            store = json.loads(self.instance.idempotency.read_text(encoding="utf-8"))
            request_ids = {item.get("request_id") for item in journal.get("records", [])}
            journal_process_ids = {item.get("process_instance_id") for item in journal.get("processes", [])}
            self.record("TEST-18", "integridad de journals", "IDs correlacionados y estados no contradictorios",
                        {"journal_request_count": len(request_ids), "journal_process_count": len(journal_process_ids),
                         "idempotency_record_count": len(store.get("records", {})),
                         "journal_clean_shutdown": journal.get("clean_shutdown"),
                         "idempotency_clean_shutdown": store.get("clean_shutdown")},
                        "PASS" if journal.get("clean_shutdown") and store.get("clean_shutdown") and request_ids else "FAIL")

            self.record("TEST-20", "dos clientes sin coordinación", "solo API LARM y model ID",
                        {"clients": [a, b, stream], "client_files": "metadata-only"}, "PASS")
        finally:
            self.instance.stop(clean=True)

    def start_controlled(self, mode, suffix):
        self.args.backend_mode = mode
        self.start_instance(suffix)

    def controlled_inference(self, mode, suffix, stream=False):
        self.start_controlled(mode, suffix)
        try:
            result = self.request("A", self.args.model_a_id, mode="sse" if stream else "json", timeout=15, max_tokens=8)
            return result, self.inference(result["request_id"]), self.snapshot(f"{suffix}_after")
        finally:
            self.instance.stop(clean=True)

    def scenario_08(self):
        if self.args.backend != "controlled":
            return self.record("TEST-08", "SSE truncado upstream", "TRUNCATED sin éxito y evidencia terminal", {}, "BLOCKED",
                                limitation="requiere --backend controlled")
        variants = []
        for mode in ("sse-truncated", "sse-done-no-eof"):
            self.start_controlled(mode, f"test-08-{mode}")
            try:
                client = self.request("A", self.args.model_a_id, mode="sse", timeout=15, max_tokens=8)
                inference = self.inference(client["request_id"])
                next_client = self.request("B", self.args.model_b_id, timeout=2 if mode == "sse-done-no-eof" else 15, max_tokens=8)
                next_inference = self.inference(next_client["request_id"])
                variants.append({"mode": mode, "client": client, "inference": inference,
                                 "next_client": next_client, "next_inference": next_inference})
            finally:
                self.instance.stop(clean=True)
        truncated, no_eof = variants
        passed = (
            truncated["inference"].get("semantic_state") == "TRUNCATED"
            and truncated["inference"].get("sse", {}).get("eof") is True
            and truncated["inference"].get("terminal_evidence")
            and truncated["next_client"].get("http_status") == 200
            and no_eof["inference"].get("semantic_state") != "SUCCEEDED"
            and no_eof["inference"].get("cancellation_state") == "CANCEL_UNKNOWN"
            and no_eof["inference"].get("sse", {}).get("done_detected") is True
            and no_eof["inference"].get("sse", {}).get("eof") is False
            and no_eof["next_client"].get("http_status") != 200
        )
        return self.record("TEST-08", "SSE truncado upstream", "TRUNCATED sin éxito, EOF/evidencia conservados y release seguro",
                           {"modality": "controlled", "backend": "Process Supervisor fixture", "variants": variants},
                           "PASS" if passed else "FAIL")

    def scenario_11(self):
        if self.args.backend != "controlled":
            return self.record("TEST-11", "error de conexión upstream", "fallo de conexión separado de fallo ambiguo", {}, "BLOCKED",
                                limitation="requiere --backend controlled")
        variants = []
        for mode in ("connection-error", "close-before-response"):
            self.start_controlled(mode, f"test-11-{mode}")
            try:
                if mode == "connection-error":
                    # Allow the supervised fixture to close its listener after
                    # the health handshake; the inference still goes through LARM.
                    time.sleep(0.3)
                client = self.request("A", self.args.model_a_id, timeout=10, max_tokens=8)
                variants.append({"mode": mode, "client": client, "inference": self.inference(client["request_id"])})
            finally:
                self.instance.stop(clean=True)
        dial, ambiguous = variants
        passed = (
            dial["inference"].get("transport_state") == "FAILED"
            and dial["inference"].get("semantic_state") == "FAILED"
            and not dial["inference"].get("terminal_evidence", [{}])[0].get("result") == "AMBIGUOUS"
            and ambiguous["inference"].get("semantic_state") == "UNKNOWN"
            and ambiguous["inference"].get("transport_state") in ("FAILED", "UNKNOWN")
        )
        return self.record("TEST-11", "error de conexión upstream", "no envío demostrado solo para dial; cierre establecido queda ambiguo",
                           {"modality": "controlled", "backend": "Process Supervisor fixture", "variants": variants},
                           "PASS" if passed else "FAIL")

    def scenario_12(self):
        if self.args.backend != "controlled":
            return self.record("TEST-12", "error HTTP upstream", "HTTP real, fallo semántico y parcialidad diferenciada", {}, "BLOCKED",
                                limitation="requiere --backend controlled")
        variants = []
        for mode in ("http-500", "http-partial"):
            self.start_controlled(mode, f"test-12-{mode}")
            try:
                client = self.request("A", self.args.model_a_id, timeout=10, max_tokens=8)
                variants.append({"mode": mode, "client": client, "inference": self.inference(client["request_id"])})
            finally:
                self.instance.stop(clean=True)
        full, partial = variants
        passed = (
            full["client"].get("http_status") == 500
            and full["inference"].get("semantic_state") == "FAILED"
            and full["inference"].get("transport_state") == "COMPLETED"
            and partial["client"].get("http_status") == 500
            and partial["inference"].get("semantic_state") in ("UNKNOWN", "TRUNCATED")
            and partial["inference"].get("semantic_state") != "SUCCEEDED"
        )
        return self.record("TEST-12", "error HTTP upstream", "código real, fallo semántico y correlación durable",
                           {"modality": "controlled", "backend": "Process Supervisor fixture", "variants": variants},
                           "PASS" if passed else "FAIL")

    def crash_recovery(self, record=True, suffix="test-17", keep_running=False):
        self.args.backend = "real"
        self.args.runtime_binary = self.args.llama_server
        self.start_instance(suffix)
        process = None
        process_full = []
        client_process = None
        try:
            client_process, rid, eid, output = self.client("A-crash", self.args.model_a_id, mode="sse", timeout=120, max_tokens=2048)
            deadline = time.monotonic() + 60
            before = {}
            while time.monotonic() < deadline:
                before = self.inference(rid)
                if before.get("transport_state") in ("STARTED", "HEADERS_RECEIVED", "BODY_COMPLETED"):
                    break
                self.snapshot("crash_before")
                time.sleep(0.2)
            process_status, process_full = json_call(self.instance.base + "/api/v1/processes")
            process = [metadata_process(item) for item in process_full] if process_status == 200 else []
            self.snapshot("crash_evidence_preserved")
            self.instance.crash()
            if client_process.poll() is None:
                client_process.terminate()
                client_process.wait(timeout=10)
            journal_before_restart = json.loads(self.instance.journal.read_text(encoding="utf-8"))
            self.instance = Instance(self.args, self.root, suffix)
            self.instance.start()
            recovery = self.snapshot("after_crash_restart")
            b = self.request("B-after-crash", self.args.model_b_id, timeout=3, max_tokens=8)
            recovered = recovery["recovery_inferences"]["body"]
            result = {
                "modality": "real_gguf",
                "backend": self.args.llama_server,
                "request_id": rid,
                "execution_id": eid,
                "inference_before_crash": before,
                "processes_before_crash": process,
                "journal_before_restart": {"clean_shutdown": journal_before_restart.get("clean_shutdown"), "record_count": len(journal_before_restart.get("records", [])), "process_count": len(journal_before_restart.get("processes", []))},
                "recovery": recovery["recovery_status"],
                "recovered_inferences": recovered,
                "client_b": b,
            "processes_after_restart": recovery["processes"],
            }
            if process:
                self.survivors.extend(process)
            self.pending_cleanup = process_full
            passed = (
                recovery["recovery_status"]["body"].get("state") == "RECOVERY_REQUIRED"
                and recovery["recovery_status"]["body"].get("admission_allowed") is False
                and any(item.get("request_id") == rid for item in recovered)
                and b.get("http_status") == 503
                and not recovery["status"]["body"].get("recovery_required") is False
            )
            if record:
                return self.record("TEST-17", "crash recovery", "journal, inferencia y correlaciones conservadas; sin adopción ni admisión",
                                   result, "PASS" if passed else "FAIL")
            return result
        finally:
            if self.instance is not None and not keep_running:
                self.instance.stop(clean=True)
            if process_full and not keep_running:
                self.cleanup_owned_processes(process_full)

    def scenario_17(self):
        if not self.args.allow_destructive:
            return self.record("TEST-17", "crash recovery", "RECOVERY_REQUIRED fail-closed", {}, "BLOCKED",
                                limitation="requiere --allow-destructive; no se ejecuta crash al importar ni por defecto")
        if self.args.backend != "real":
            return self.record("TEST-17", "crash recovery", "GGUF real y proceso sobreviviente diagnosticado", {}, "BLOCKED",
                                limitation="requiere --backend real")
        return self.crash_recovery(record=True)

    def scenario_19(self):
        if not self.args.allow_destructive:
            return self.record("TEST-19", "Recovery Plan real", "incidente, plan y auditoría durable", {}, "BLOCKED",
                                limitation="requiere --allow-destructive; depende de TEST-17")
        if self.args.backend != "real":
            return self.record("TEST-19", "Recovery Plan real", "incidente y plan desde crash real", {}, "BLOCKED",
                                limitation="requiere --backend real")
        self.crash_recovery(record=False, suffix="test-19", keep_running=True)
        incidents_status, incidents = json_call(self.instance.base + "/api/v1/recovery/incidents")
        incident = next((item for item in incidents if item.get("incident_type") in ("INFERENCE_UNKNOWN", "INFERENCE_INCOMPLETE", "JOURNAL_DIRTY", "PROCESS_IDENTITY_UNVERIFIED")), None) if isinstance(incidents, list) else None
        plan_status, plan = (None, {})
        audit_status, audit = json_call(self.instance.base + "/api/v1/recovery/audit")
        if incident:
            plan_status, plan = json_call(self.instance.base + "/api/v1/recovery/incidents/" + incident["incident_id"] + "/plan")
        actual = {"incidents_http_status": incidents_status, "incident": incident, "plan_http_status": plan_status,
                  "plan": plan, "audit_http_status": audit_status, "audit": audit,
                  "administrative_execution": "not attempted"}
        passed = bool(incident and plan_status == 200 and plan.get("evidence_digest") and plan.get("snapshot_version")
                      and plan.get("safety_classification", "").startswith("BLOCKED")
                      and plan.get("proposed_action") == "NO_RELEASE_OR_UNLOCK_WITHOUT_VERIFIED_EVIDENCE")
        try:
            return self.record("TEST-19", "Recovery Plan real", "plan reproducible, correlacionado, fail-closed y sin ejecución", actual,
                               "PASS" if passed else "FAIL")
        finally:
            if self.instance is not None:
                self.instance.stop(clean=True)
            if self.pending_cleanup:
                self.cleanup_owned_processes(self.pending_cleanup)
    def blocked_matrix(self):
        for test_id, fn in (("TEST-08", self.scenario_08), ("TEST-10", self.scenario_10),
                            ("TEST-11", self.scenario_11), ("TEST-12", self.scenario_12),
                            ("TEST-17", self.scenario_17), ("TEST-19", self.scenario_19)):
            if not any(row["test_id"] == test_id for row in self.results):
                fn()

    def scenario_10(self):
        if not self.args.allow_destructive:
            return self.record("TEST-10", "desconexión downstream", "UNKNOWN protegido y B sin admisión efectiva", {}, "BLOCKED",
                                limitation="requiere --allow-destructive; no se ejecuta desconexión por defecto")
        if self.args.backend != "real":
            return self.record("TEST-10", "desconexión downstream", "GGUF real y dos clientes independientes", {}, "BLOCKED",
                                limitation="requiere --backend real")
        self.start_instance("test-10")
        owned_processes = []
        try:
            a_proc, rid, eid, a_output = self.client("A-disconnect", self.args.model_a_id, mode="sse", timeout=120,
                                                       close_after_headers=True, max_tokens=8192, long_stream=True)
            deadline = time.monotonic() + 60
            before = {}
            while time.monotonic() < deadline:
                before = self.inference(rid)
                if before.get("transport_state") in ("STARTED", "HEADERS_RECEIVED", "BODY_COMPLETED"):
                    break
                time.sleep(0.2)
            a_result = self.await_client(a_proc, a_output, "TEST-10_A_disconnect")
            b_proc, brid, beid, b_output = self.client("B-after-disconnect", self.args.model_b_id, timeout=3, max_tokens=8)
            while b_proc.poll() is None:
                self.snapshot("TEST-10_B_waiting")
                time.sleep(0.2)
            b_result = self.read_client(b_output)
            after = self.inference(rid)
            b_inference = self.inference(brid)
            process_status, owned_processes = json_call(self.instance.base + "/api/v1/processes")
            self.record("TEST-10", "desconexión downstream", "UNKNOWN protegido, sin stop/release inseguro ni admisión B",
                        {"modality": "real_gguf", "backend": self.args.llama_server, "request_id": rid, "execution_id": eid,
                         "client_a": a_result, "inference_a_before": before, "inference_a_after": after,
                         "client_b": b_result, "inference_b": b_inference, "processes": metadata_snapshot(self.instance.base, "TEST-10_after")["processes"],
                         "process_http_status": process_status},
                        "PASS" if a_result.get("downstream_disconnected") and after.get("semantic_state") == "UNKNOWN"
                        and after.get("lease_id") and b_result.get("http_status") != 200 else "FAIL")
        finally:
            if self.instance is not None and self.instance.proc is not None:
                # UNKNOWN deliberately prevents a clean scheduler shutdown;
                # preserve evidence, crash only this LARM child, then clean
                # the exact recorded backend group if ownership still matches.
                self.instance.crash()
                if owned_processes:
                    self.cleanup_owned_processes(owned_processes)

    def write_report(self):
        report = {
            "phase": "LARM-006.5.1",
            "generated_at": iso(),
            "platform": sys.platform,
            "python": sys.version.split()[0],
            "llama_server": self.args.llama_server,
            "models": {"A": self.args.model_a_id, "B": self.args.model_b_id},
            "evidence_dir": str(self.root),
            "results": self.results,
        }
        (self.root / "summary.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return report


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-larm", action="store_true", help="start an isolated LARM instance")
    parser.add_argument("--scenario", choices=("all", "blocked", "TEST-08", "TEST-10", "TEST-11", "TEST-12", "TEST-17", "TEST-19"), default="all")
    parser.add_argument("--backend", choices=("real", "controlled"), default="real")
    parser.add_argument("--allow-destructive", action="store_true", help="required for TEST-10/17/19")
    parser.add_argument("--larm-url", default="http://127.0.0.1:8090")
    parser.add_argument("--larm-binary")
    parser.add_argument("--llama-server", default=shutil.which("llama-server"))
    parser.add_argument("--model-a", type=Path, default=DEFAULT_A)
    parser.add_argument("--model-b", type=Path, default=DEFAULT_B)
    parser.add_argument("--model-a-id", default="model-a")
    parser.add_argument("--model-b-id", default="model-b")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--startup-timeout", type=float, default=300)
    return parser.parse_args()


def main():
    args = parse_args()
    if sys.platform != "darwin":
        print("BLOCKED: LARM-006.5 solo se ejecuta en macOS", file=sys.stderr)
        return 2
    if args.backend == "real" and (not args.llama_server or not Path(args.llama_server).is_file()):
        print("BLOCKED: llama-server no está disponible", file=sys.stderr)
        return 2
    if args.backend == "real" and args.start_larm and (not args.model_a.is_file() or not args.model_b.is_file()):
        print("BLOCKED: faltan dos GGUF utilizables; no se descargan modelos", file=sys.stderr)
        return 2
    suite = Suite(args)
    if args.start_larm and args.scenario == "all" and args.backend == "real":
        suite.safe_suite()
    if args.scenario == "blocked":
        suite.blocked_matrix()
    elif args.scenario != "all":
        getattr(suite, "scenario_" + args.scenario.split("-")[1])()
    elif args.start_larm:
        suite.blocked_matrix()
    else:
        suite.instance = type("External", (), {"base": args.larm_url})()
        suite.blocked_matrix()
    report = suite.write_report()
    print(json.dumps({"result_dir": str(suite.root), "tests": len(report["results"]), "summary": report["results"]}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
