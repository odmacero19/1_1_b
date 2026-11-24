#!/usr/bin/env python3
"""
Minimal stand-in for the delta sync endpoint (default app setting: http://10.0.2.2:8080/mock-sync/v1,
10.0.2.2 is the host machine as seen from the Android emulator).

  POST /mock-sync/v1/push   -> 200, body is the protobuf DeltaPushRequest (size + device id are logged)
  GET  /mock-sync/v1/pull   -> 204 (nothing new)

Failure injection, to exercise the retry path of the push job:
  python3 tools/mock_sync_server.py --fail 503            # every push fails with 503
  python3 tools/mock_sync_server.py --fail 503 --times 3  # only the first 3 pushes fail
  python3 tools/mock_sync_server.py --fail 422            # permanent rejection, entries get parked
"""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

args = None
received = []
failures_left = 0


class Handler(BaseHTTPRequestHandler):
    def _reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        global failures_left
        size = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(size)
        device = self.headers.get("X-Device-Id", "?")
        if args.fail and (args.times == 0 or failures_left > 0):
            failures_left -= 1
            print(f"push from {device}: {size} bytes -> injected HTTP {args.fail}", flush=True)
            return self._reply(args.fail)
        received.append(body)
        print(f"push from {device}: {size} bytes -> 200 (total {len(received)} requests)", flush=True)
        self._reply(200)

    def do_GET(self):
        print(f"pull {self.path} -> 204", flush=True)
        self._reply(204)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--fail", type=int, default=0)
    p.add_argument("--times", type=int, default=0, help="0 = always")
    args = p.parse_args()
    failures_left = args.times
    print(f"mock sync endpoint on :{args.port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()
