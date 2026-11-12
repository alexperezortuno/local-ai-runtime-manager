#!/usr/bin/env python3
"""Small HTTP backend used only through LARM's real Process Supervisor.

The fixture deliberately exposes no control endpoint. Its mode is fixed at
startup and all behavior is exercised through the model port LARM owns.
"""

import argparse
import http.server
import socket
import threading
import time


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "larm-fixture/1"

    def log_message(self, _format, *_args):
        return

    def do_GET(self):  # noqa: N802 - stdlib handler API
        if self.path != "/health":
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}\n')
        self.wfile.flush()
        if self.server.mode == "connection-error" and not self.server.health_closed:
            self.server.health_closed = True
            threading.Thread(target=self.server.close_listener, daemon=True).start()

    def do_POST(self):  # noqa: N802 - stdlib handler API
        mode = self.server.mode
        if mode == "connection-error":
            self.close_without_headers()
            return
        if mode == "close-before-response":
            self.close_without_headers()
            return
        if mode == "sse-truncated":
            self.sse([b'data: {"choices":[{"delta":{"content":"one"}}]}\n\n'])
            return
        if mode == "sse-done-no-eof":
            self.sse([
                b'data: {"choices":[{"delta":{"content":"one"}}]}\n\n',
                b"data: [DONE]\n\n",
            ], keep_open=True)
            return
        if mode == "http-500":
            self.send_json(500, b'{"error":{"message":"fixture failure"}}\n')
            return
        if mode == "http-partial":
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "4096")
            self.end_headers()
            self.wfile.write(b'{"error":"partial')
            self.wfile.flush()
            self.close_without_headers()
            return
        self.send_json(200, b'{"id":"fixture","choices":[{"message":{"content":"ok"}}]}\n')

    def sse(self, chunks, keep_open=False):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        for chunk in chunks:
            self.wfile.write(chunk)
            self.wfile.flush()
        if keep_open:
            time.sleep(self.server.hold_seconds)

    def send_json(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def close_without_headers(self):
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.connection.close()


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, mode, hold_seconds):
        super().__init__(address, Handler)
        self.mode = mode
        self.hold_seconds = hold_seconds
        self.health_closed = False

    def close_listener(self):
        time.sleep(0.05)
        self.server_close()
        self.shutdown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--fixture-mode", required=True)
    parser.add_argument("--hold-seconds", type=float, default=60.0)
    args, _ = parser.parse_known_args()
    server = Server(("127.0.0.1", args.port), args.fixture_mode, args.hold_seconds)
    try:
        server.serve_forever()
    finally:
        server.server_close()
    # connection-error intentionally leaves the supervised process alive after
    # closing its listener, so LARM observes a real dial failure.
    if args.fixture_mode == "connection-error":
        time.sleep(args.hold_seconds)


if __name__ == "__main__":
    main()
