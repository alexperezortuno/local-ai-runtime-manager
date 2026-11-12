#!/usr/bin/env python3
"""Submit an OpenCode prompt through LARM's OpenAI-compatible proxy."""

import argparse
import base64
import http.client
import json
import os
import signal
import sys
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse


def opencode(base, method, path, value=None, timeout=30):
    url = urlparse(base)
    conn = http.client.HTTPConnection(url.hostname, url.port, timeout=timeout)
    payload = None if value is None else json.dumps(value).encode()
    headers = {"Content-Type": "application/json"} if payload else {}
    password = os.environ.get("OPENCODE_SERVER_PASSWORD")
    if password:
        username = os.environ.get("OPENCODE_SERVER_USERNAME", "opencode")
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = "Basic " + token
    conn.request(method, url.path.rstrip("/") + path, payload, headers)
    response = conn.getresponse()
    body = response.read()
    conn.close()
    try:
        parsed = json.loads(body) if body else None
    except json.JSONDecodeError:
        parsed = body.decode(errors="replace")
    return response.status, parsed


def unwrap(value):
    return value.get("data", value) if isinstance(value, dict) else value


def emit(kind, **fields):
    path = os.environ.get("LARM_OPENCODE_LAUNCHER_EVIDENCE", os.environ.get("LARM_OPENCODE_EVIDENCE"))
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(json.dumps({"type": kind, "at": datetime.now(timezone.utc).isoformat(), **fields}) + "\n")
    except OSError:
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, help="OpenCode v2 API base URL")
    parser.add_argument("--model", required=True, help="exact LARM model ID")
    parser.add_argument("--provider-id", default=os.environ.get("LARM_OPENCODE_PROVIDER_ID"), required=not bool(os.environ.get("LARM_OPENCODE_PROVIDER_ID")))
    parser.add_argument("--session", help="existing OpenCode session; omitted creates one")
    parser.add_argument("--directory", default=os.getcwd())
    parser.add_argument("--timeout", type=int, default=120)
    # Kept only so old scripts fail neither parsing nor silently regain authority.
    parser.add_argument("--server-id", help=argparse.SUPPRESS)
    parser.add_argument("--coordinator-socket", help=argparse.SUPPRESS)
    parser.add_argument("--larm-api", help=argparse.SUPPRESS)
    args = parser.parse_args()

    interrupted = False

    def on_terminate(_signum, _frame):
        nonlocal interrupted
        interrupted = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, on_terminate)
    prompt = sys.stdin.read()
    if not prompt.strip():
        parser.error("read a non-empty prompt from stdin")

    execution_id = str(uuid.uuid4())
    session_id = args.session
    prompt_started = False
    try:
        if not session_id:
            status, response = opencode(args.server, "POST", "/api/session", {
                "title": "LARM managed execution",
                "location": {"directory": os.path.abspath(args.directory)},
            })
            if status < 200 or status >= 300:
                raise RuntimeError(f"session creation failed with HTTP {status}")
            session_id = unwrap(response)["id"]

        emit("session_declared", execution_id=execution_id, session_id=session_id, model_id=args.model, ownership="DECLARED_UNVERIFIED")
        prompt_started = True
        status, _ = opencode(args.server, "POST", f"/api/session/{session_id}/prompt", {
            "text": prompt,
            "model": {"providerID": args.provider_id, "modelID": args.model},
            "metadata": {"larm_execution": {"execution_id": execution_id, "model_id": args.model, "provider_id": args.provider_id}},
        }, timeout=args.timeout)
        if status < 200 or status >= 300:
            raise RuntimeError(f"prompt submission failed with HTTP {status}")
        emit("prompt_accepted", execution_id=execution_id, session_id=session_id, http_status=status)
        wait_status, _ = opencode(args.server, "POST", f"/api/experimental/session/{session_id}/wait", timeout=args.timeout)
        if wait_status < 200 or wait_status >= 300:
            raise RuntimeError(f"execution wait failed with HTTP {wait_status}")
        emit("opencode_wait_returned", execution_id=execution_id, session_id=session_id, http_status=wait_status)
        print(json.dumps({"result": "OPEN_CODE_WAIT_RETURNED", "execution_id": execution_id, "session_id": session_id, "model_id": args.model, "upstream_state": "CONSULT_LARM"}))
        return 0
    except KeyboardInterrupt:
        if prompt_started and session_id:
            try:
                status, response = opencode(args.server, "POST", f"/api/session/{session_id}/interrupt", {}, timeout=5)
                emit("interrupt_requested", execution_id=execution_id, session_id=session_id, http_status=status, opencode_result=response)
            except Exception as error:
                emit("interrupt_request_failed", execution_id=execution_id, session_id=session_id, error=type(error).__name__)
        print(json.dumps({"result": "OPEN_CODE_INTERRUPT_REQUESTED", "execution_id": execution_id, "session_id": session_id, "model_id": args.model, "upstream_state": "CONSULT_LARM"}))
        return 130
    except Exception as error:
        emit("launcher_error", execution_id=execution_id, session_id=session_id, prompt_started=prompt_started, error=type(error).__name__)
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"larm-opencode-launcher: {type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1)
