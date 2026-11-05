#!/usr/bin/env python3
"""One deliberately independent HTTP client for LARM-006.5.

The client only knows the LARM URL and a model ID. It never contacts
llama-server and writes metadata only; response bodies are read and discarded.
"""

import argparse
import json
import socket
import struct
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone


def now():
    return datetime.now(timezone.utc).isoformat()


def request_id(value):
    return value or str(uuid.uuid4())


def write_result(path, result):
    with open(path, "w", encoding="utf-8") as output:
        json.dump(result, output, sort_keys=True)
        output.write("\n")
        output.flush()


def disconnect_socket(response):
    """Close the exact HTTP socket so the server observes downstream loss."""
    try:
        sock = response.fp.raw._sock
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        sock.close()
    except (AttributeError, OSError):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--request-id")
    parser.add_argument("--execution-id")
    parser.add_argument("--lease-id")
    parser.add_argument("--mode", choices=("json", "sse"), default="json")
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--close-after-events", type=int, default=0)
    parser.add_argument("--close-after-headers", action="store_true")
    parser.add_argument("--long-stream", action="store_true")
    args = parser.parse_args()

    sent_at = now()
    started = time.monotonic()
    rid = request_id(args.request_id)
    eid = args.execution_id or str(uuid.uuid4())
    result = {
        "client_id": args.client_id,
        "request_id": rid,
        "execution_id": eid,
        "model_id": args.model,
        "sent_at": sent_at,
        "response_at": None,
        "http_status": None,
        "mode": args.mode,
        "duration_ms": None,
        "terminal_observed": False,
        "body_completed": False,
        "sse_done": False,
        "sse_eof": False,
        "downstream_disconnected": False,
        "error_type": None,
        "error": None,
    }
    body = json.dumps({
        "model": args.model,
        "messages": [{"role": "user", "content": "LARM integration probe. Generate a long deterministic response." if args.long_stream else "LARM-006.5 integration probe."}],
        "stream": args.mode == "sse",
        "max_tokens": args.max_tokens,
        "temperature": 0,
    }).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream" if args.mode == "sse" else "application/json",
        "X-LARM-Client-ID": args.client_id,
        "X-LARM-Request-ID": rid,
        "X-LARM-Execution-ID": eid,
    }
    if args.lease_id:
        headers["X-LARM-Lease-ID"] = args.lease_id
    request = urllib.request.Request(
        args.url.rstrip("/") + "/v1/chat/completions",
        data=body,
        headers=headers,
        method="POST",
    )
    response = None
    try:
        response = urllib.request.urlopen(request, timeout=args.timeout)
        result["http_status"] = response.status
        if args.close_after_headers:
            result["downstream_disconnected"] = True
            disconnect_socket(response)
            return 0
        if args.mode == "json":
            response.read()  # The body is intentionally not persisted.
            result["body_completed"] = True
            result["terminal_observed"] = True
        else:
            events = 0
            while True:
                line = response.readline()
                if not line:
                    result["sse_eof"] = True
                    break
                if line.startswith(b"data:"):
                    data = line[5:].strip()
                    if data == b"[DONE]":
                        result["sse_done"] = True
                    elif data:
                        events += 1
                if args.close_after_events and events >= args.close_after_events:
                    result["downstream_disconnected"] = True
                    disconnect_socket(response)
                    break
            result["terminal_observed"] = result["sse_done"] and result["sse_eof"]
    except urllib.error.HTTPError as error:
        result["http_status"] = error.code
        result["error_type"] = "HTTPError"
        result["error"] = "upstream_http_error"
        try:
            error.read()  # Do not persist an upstream error body.
        except Exception:
            pass
    except Exception as error:  # noqa: BLE001 - metadata-only client diagnostic
        result["error_type"] = type(error).__name__
        result["error"] = "transport_error"
    finally:
        if response is not None:
            response.close()
        result["response_at"] = now()
        result["duration_ms"] = round((time.monotonic() - started) * 1000, 1)
        write_result(args.output, result)

    return 0 if result["http_status"] is not None else 1


if __name__ == "__main__":
    sys.exit(main())
