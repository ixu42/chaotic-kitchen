from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# (label, argv relative to python)
WORKERS: list[tuple[str, list[str]]] = [
    ("router", ["router/main.py"]),
    ("grill", ["workers/station.py", "grill"]),
    ("drinks", ["workers/station.py", "drinks"]),
    ("dessert", ["workers/station.py", "dessert"]),
    ("retry", ["workers/retry.py"]),
]


def _stream(label: str, pipe) -> None:
    for raw in iter(pipe.readline, b""):
        line = raw.decode("utf-8", errors="replace").rstrip("\n\r")
        print(f"[{label}] {line}", flush=True)
    pipe.close()


def _start(label: str, script_argv: list[str]) -> subprocess.Popen:
    # -u / PYTHONUNBUFFERED: children write through pipes line-by-line (not block-buffered)
    cmd = [sys.executable, "-u", *script_argv]
    env = os.environ.copy()
    env["KITCHEN_RUNNER"] = "1"
    env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        bufsize=0,
    )
    assert proc.stdout is not None
    threading.Thread(target=_stream, args=(label, proc.stdout), daemon=True).start()
    print(f"[runner] started {label} (pid {proc.pid})", flush=True)
    return proc


def _shutdown(procs: list[tuple[str, subprocess.Popen]], *, force: bool = False) -> None:
    for _label, proc in procs:
        if proc.poll() is not None:
            continue
        try:
            if force:
                proc.kill()
            else:
                proc.terminate()
        except OSError:
            continue
    if not force:
        deadline = time.time() + 3.0
        for label, proc in procs:
            remaining = max(0.0, deadline - time.time())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                print(f"[runner] force-killing {label}", flush=True)
                proc.kill()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Start Chaotic Kitchen workers (router, stations, retry)."
    )
    parser.parse_args()

    print("[runner] Chaotic Kitchen — Ctrl+C to stop all workers", flush=True)
    print("[runner] Board (separate):  python board/main.py", flush=True)
    print("[runner] Producer (separate): python producer/main.py 2", flush=True)

    procs: list[tuple[str, subprocess.Popen]] = []
    stopping = False

    def on_signal(_signum, _frame) -> None:
        nonlocal stopping
        if stopping:
            return
        stopping = True
        print("\n[runner] shutting down…", flush=True)
        _shutdown(procs)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    try:
        for label, argv in WORKERS:
            if stopping:
                break
            procs.append((label, _start(label, argv)))
            time.sleep(0.15)

        exit_code = 0
        while not stopping and procs:
            for label, proc in procs:
                code = proc.poll()
                if code is None:
                    continue
                if stopping:
                    break
                print(
                    f"[runner] {label} exited unexpectedly (code {code}); stopping others",
                    flush=True,
                )
                stopping = True
                exit_code = code if code != 0 else 1
                _shutdown([(l, p) for l, p in procs if l != label])
                break
            else:
                time.sleep(0.2)
                continue
            break

        _shutdown(procs)
        return exit_code
    except KeyboardInterrupt:
        _shutdown(procs)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
