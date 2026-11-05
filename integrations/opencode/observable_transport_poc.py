#!/usr/bin/env python3
"""Isolated, metadata-only HTTP/SSE transport observer for the OpenCode PoC.

This is deliberately not an LARM component: it owns no lease, scheduler, or
coordinator state.  It only forwards one configured loopback upstream and
records transport evidence.
"""

from __future__ import annotations

import http.client
import json
import queue
import select
import socket
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
MAX_BODY = 16 << 20


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class RequestContext:
    transport_id: str
    execution_id: str | None
    session_id: str | None
    request_id: str
    request_id_source: str
    lease_id: str | None
    model_id: str | None
    method: str
    path: str
    stream: bool
    body_size: int
    evidence: str | None
    upstream_conn: http.client.HTTPConnection | None = None
    downstream_closed: bool = False
    cancel_requested: bool = False
    upstream_headers: bool = False
    sse_done: bool = False
    terminal: str | None = None
    done: threading.Event = field(default_factory=threading.Event)
    stop_watcher: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)


class ObservableHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler, transport):
        super().__init__(address, handler)
        self.transport = transport


class ObservableTransport:
    """A single-upstream HTTP proxy used only by isolated fixtures."""

    def __init__(self, upstream: str, evidence: str | None = None):
        parsed = urlsplit(upstream)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"}:
            raise ValueError("PoC upstream must be an explicit loopback http URL")
        if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError("PoC upstream must contain only scheme, host and port")
        if parsed.port is None:
            raise ValueError("PoC upstream port is required")
        self.host = parsed.hostname
        self.upstream_port = parsed.port
        self.evidence = evidence
        self.events: list[dict] = []
        self.contexts: dict[str, RequestContext] = {}
        self.lock = threading.Lock()
        self.server = ObservableHTTPServer(("127.0.0.1", 0), ObservableHandler, self)
        self.thread = threading.Thread(target=self.server.serve_forever, name="observable-transport", daemon=True)

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        self.thread.start()

    def emit(self, ctx: RequestContext, event: str, **fields) -> None:
        row = {
            "type": event,
            "at": now(),
            "transport_id": ctx.transport_id,
            "execution_id": ctx.execution_id,
            "session_id": ctx.session_id,
            "request_id": ctx.request_id,
            "lease_id": ctx.lease_id,
            "model_id": ctx.model_id,
            **fields,
        }
        with self.lock:
            self.events.append(row)
            if self.evidence:
                with open(self.evidence, "a", encoding="utf-8") as stream:
                    stream.write(json.dumps(row, sort_keys=True) + "\n")

    def register(self, ctx: RequestContext) -> None:
        with self.lock:
            self.contexts[ctx.transport_id] = ctx

    def unregister(self, ctx: RequestContext) -> None:
        with self.lock:
            self.contexts.pop(ctx.transport_id, None)

    def close_downstream(self, ctx: RequestContext, source: str) -> None:
        with ctx.lock:
            if ctx.downstream_closed:
                return
            ctx.downstream_closed = True
            ctx.cancel_requested = True
            connection = ctx.upstream_conn
        self.emit(ctx, "DOWNSTREAM_CLOSED", source=source)
        if connection is not None:
            try:
                connection.close()
            except OSError:
                pass

    def settle(self, ctx: RequestContext, diagnosis: str, settled: bool = True, **fields) -> None:
        with ctx.lock:
            if ctx.terminal is not None:
                self.emit(ctx, "REQUEST_TERMINAL_DUPLICATE", diagnosis=diagnosis, first_diagnosis=ctx.terminal)
                return
            ctx.terminal = diagnosis
        self.emit(ctx, "REQUEST_SETTLED" if settled else "REQUEST_UNKNOWN", diagnosis=diagnosis, **fields)

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        with self.lock:
            active = list(self.contexts.values())
        for ctx in active:
            if ctx.terminal is None:
                self.settle(ctx, "upstream_not_terminal_at_fixture_shutdown", settled=False)
            ctx.stop_watcher.set()
            connection = ctx.upstream_conn
            if connection is not None:
                try:
                    connection.close()
                except OSError:
                    pass
        self.thread.join(timeout=2)


class ObservableHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    @property
    def transport(self) -> ObservableTransport:
        return self.server.transport  # type: ignore[attr-defined]

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def do_PUT(self):
        self.forward()

    def do_PATCH(self):
        self.forward()

    def do_DELETE(self):
        self.forward()

    def do_OPTIONS(self):
        self.forward()

    def _identity(self, body: bytes) -> tuple[str | None, str | None, str, str, str | None, str | None, bool]:
        headers = self.headers
        execution_id = headers.get("X-LARM-Execution-ID")
        session_id = headers.get("X-LARM-Session-ID") or headers.get("X-OpenCode-Session-ID")
        supplied_request_id = headers.get("X-LARM-Request-ID")
        request_id = supplied_request_id or str(uuid.uuid4())
        request_id_source = "header" if supplied_request_id else "adapter_generated"
        lease_id = headers.get("X-LARM-Lease-ID")
        model_id = None
        stream = False
        try:
            value = json.loads(body)
            if isinstance(value, dict):
                model_id = value.get("model") if isinstance(value.get("model"), str) else None
                stream = value.get("stream") is True
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
        return execution_id, session_id, request_id, request_id_source, lease_id, model_id, stream

    def _read_body(self) -> bytes:
        length = self.headers.get("Content-Length")
        if length is None:
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                raise ValueError("chunked downstream requests are not supported by this PoC")
            return b""
        size = int(length)
        if size < 0 or size > MAX_BODY:
            raise ValueError("request body exceeds PoC limit")
        return self.rfile.read(size)

    def _watch_downstream(self, ctx: RequestContext) -> None:
        while not ctx.stop_watcher.wait(0.05):
            try:
                readable, _, _ = select.select([self.connection], [], [], 0)
                if not readable:
                    continue
                data = self.connection.recv(1, socket.MSG_PEEK)
                if not data:
                    self.transport.close_downstream(ctx, "socket_eof")
                    return
            except (OSError, ValueError):
                self.transport.close_downstream(ctx, "socket_error")
                return

    def _upstream_headers(self, status: int, headers: list[tuple[str, str]]) -> None:
        self.send_response(status)
        for name, value in headers:
            if name.lower() in HOP_BY_HOP or name.lower() == "content-length":
                continue
            self.send_header(name, value)
        self.send_header("Connection", "close")
        self.end_headers()

    def forward(self):
        try:
            body = self._read_body()
        except (ValueError, OSError):
            self.send_error(400, "invalid request body")
            return
        execution_id, session_id, request_id, request_id_source, lease_id, model_id, stream = self._identity(body)
        ctx = RequestContext(
            transport_id=str(uuid.uuid4()),
            execution_id=execution_id,
            session_id=session_id,
            request_id=request_id,
            request_id_source=request_id_source,
            lease_id=lease_id,
            model_id=model_id,
            method=self.command,
            path=self.path,
            stream=stream,
            body_size=len(body),
            evidence=self.transport.evidence,
        )
        self.transport.register(ctx)
        self.transport.emit(
            ctx,
            "DOWNSTREAM_CONNECTED",
            method=self.command,
            path=self.path.split("?", 1)[0],
            query_present="?" in self.path,
            request_id_source=request_id_source,
            body_size=len(body),
        )
        upstream_path = self.path if self.path.startswith("/") else "/" + self.path
        parsed_headers = {name.lower(): value for name, value in self.headers.items()}
        upstream_headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower() not in HOP_BY_HOP and name.lower() not in {"host", "content-length"}
        }
        upstream_headers["Host"] = f"{self.transport.host}:{self.transport.upstream_port}"
        upstream_headers["Content-Length"] = str(len(body))
        result_queue: queue.Queue[tuple] = queue.Queue()
        connection = http.client.HTTPConnection(self.transport.host, self.transport.upstream_port, timeout=None)
        ctx.upstream_conn = connection
        self.transport.emit(ctx, "UPSTREAM_STARTED", body_size=len(body), authorization_present="authorization" in parsed_headers)

        def do_upstream():
            try:
                connection.request(self.command, upstream_path, body=body or None, headers=upstream_headers)
                response = connection.getresponse()
                result_queue.put(("headers", response.status, list(response.getheaders())))
                while True:
                    # read1 returns bytes already available instead of waiting
                    # for the whole buffer, which is required for SSE.
                    part = response.read1(32 * 1024) if hasattr(response, "read1") else response.read(32 * 1024)
                    if not part:
                        result_queue.put(("eof",))
                        return
                    result_queue.put(("body", part))
            except Exception as error:  # transport metadata only; never serialize the message
                result_queue.put(("error", type(error).__name__))
            finally:
                try:
                    connection.close()
                except OSError:
                    pass

        worker = threading.Thread(target=do_upstream, name=f"upstream-{ctx.transport_id}", daemon=True)
        watcher = threading.Thread(target=self._watch_downstream, args=(ctx,), name=f"downstream-{ctx.transport_id}", daemon=True)
        worker.start()
        watcher.start()
        response_started = False
        sse_pending = ""
        try:
            while True:
                kind, *value = result_queue.get()
                if kind == "headers":
                    status, headers = value
                    response_started = True
                    ctx.upstream_headers = True
                    self.transport.emit(ctx, "UPSTREAM_HEADERS", http_status=status)
                    self._upstream_headers(status, headers)
                    continue
                if kind == "body":
                    part = value[0]
                    if not response_started:
                        raise RuntimeError("body arrived before upstream headers")
                    if stream:
                        sse_pending += part.decode("utf-8", errors="replace")
                        lines = sse_pending.splitlines(keepends=True)
                        sse_pending = lines.pop() if lines and not lines[-1].endswith(("\n", "\r")) else ""
                        for line in lines:
                            if line.strip() == "data: [DONE]":
                                if not ctx.sse_done:
                                    ctx.sse_done = True
                                    self.transport.emit(ctx, "SSE_DONE")
                    try:
                        self.wfile.write(part)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        self.transport.close_downstream(ctx, "write_error")
                    continue
                if kind == "eof":
                    self.transport.emit(ctx, "UPSTREAM_EOF")
                    if stream:
                        diagnosis = "SSE_COMPLETED" if ctx.sse_done else "SSE_TRUNCATED"
                    else:
                        diagnosis = "HTTP_COMPLETED"
                    self.transport.settle(ctx, diagnosis, eof=True, sse_done=ctx.sse_done if stream else None)
                    break
                if kind == "error":
                    error_type = value[0]
                    self.transport.emit(ctx, "UPSTREAM_ERROR", error_type=error_type)
                    if ctx.downstream_closed and ctx.cancel_requested:
                        self.transport.emit(ctx, "UPSTREAM_ABORTED", error_type=error_type)
                        self.transport.settle(ctx, "CANCELLED", cancel_confirmed=True, error_type=error_type)
                    else:
                        self.transport.settle(ctx, "TRANSPORT_ERROR", transport_error=True, error_type=error_type)
                    if not response_started and not ctx.downstream_closed:
                        payload = json.dumps({"error": {"message": "upstream transport failed", "type": "proxy_error"}}).encode()
                        try:
                            self.send_response(502)
                            self.send_header("Content-Type", "application/json")
                            self.send_header("Content-Length", str(len(payload)))
                            self.send_header("Connection", "close")
                            self.end_headers()
                            self.wfile.write(payload)
                            self.wfile.flush()
                        except OSError:
                            self.transport.close_downstream(ctx, "write_error")
                    break
        except Exception as error:
            self.transport.emit(ctx, "UPSTREAM_ERROR", error_type=type(error).__name__)
            if ctx.downstream_closed and ctx.cancel_requested:
                self.transport.emit(ctx, "UPSTREAM_ABORTED", error_type=type(error).__name__)
                self.transport.settle(ctx, "CANCELLED", cancel_confirmed=True, error_type=type(error).__name__)
            else:
                self.transport.settle(ctx, "TRANSPORT_ERROR", transport_error=True, error_type=type(error).__name__)
        finally:
            if ctx.terminal is not None and not ctx.downstream_closed:
                self.transport.close_downstream(ctx, "response_complete")
            ctx.done.set()
            ctx.stop_watcher.set()
            watcher.join(timeout=1)
            worker.join(timeout=1)
            self.transport.unregister(ctx)
            self.close_connection = True


def sanitize_events(events: list[dict]) -> list[dict]:
    """Return a copy suitable for evidence/results; no body or response bytes."""
    return [dict(row) for row in events]


if __name__ == "__main__":
    import argparse
    import time

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--evidence")
    args = parser.parse_args()
    transport = ObservableTransport(args.upstream, args.evidence)
    transport.start()
    print(json.dumps({"base_url": transport.base_url, "pid": __import__("os").getpid()}), flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        transport.shutdown()
