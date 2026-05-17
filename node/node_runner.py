"""
node/node_runner.py  -- v0.8.0

Fixes vs v0.7:
1. Logs are written to logs/<label>.log AND streamed to console via a
   reader thread. subprocess.PIPE was swallowing all warnings/errors,
   making debugging impossible.
2. QKDL instances are launched per unique QKDL port before nodes start.
3. A background thread watches for unexpected process exits and prints
   the last N lines of that process's log so the cause is visible.
"""

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import yaml

ROOT        = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "nodes" / "network.yaml"
LOG_DIR     = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


#Log streaming 

def _stream_output(proc: subprocess.Popen, label: str, log_path: Path) -> None:
    """
    Read stdout+stderr from proc line by line.
    Write every line to log_path and also print it with a [label] prefix
    so all node output is visible in the terminal that ran node_runner.
    """
    log_path.parent.mkdir(exist_ok=True)
    with open(log_path, "a", buffering=1) as fh:
        for raw in proc.stdout:
            line = raw.decode(errors="replace").rstrip()
            fh.write(line + "\n")
            print(f"[{label}] {line}", flush=True)


def _tail(log_path: Path, n: int = 20) -> list[str]:
    """Return last n lines of a log file (for crash reporting)."""
    try:
        lines = log_path.read_text(errors="replace").splitlines()
        return lines[-n:]
    except Exception:
        return []


#Process launchers 

def start_qkdl(port: int) -> tuple[subprocess.Popen, Path]:
    label    = f"qkdl-{port}"
    log_path = LOG_DIR / f"{label}.log"
    env      = {**os.environ, "QKDL_PORT": str(port)}

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "qunetsim_service:app",
            "--host", "0.0.0.0",
            "--port", str(port),
            "--log-level", "warning",
        ],
        env=env,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    threading.Thread(
        target=_stream_output, args=(proc, label, log_path),
        daemon=True, name=f"log-{label}",
    ).start()
    print(f"  Started {label:<20} (port {port}) pid={proc.pid}")
    return proc, log_path


def start_node(node_cfg: dict, global_cfg: dict) -> tuple[subprocess.Popen, Path]:
    label    = node_cfg["label"]
    log_path = LOG_DIR / f"{label}.log"

    env = {**os.environ}
    env.update(node_cfg.get("env", {}))
    env.setdefault("KME_URL",  global_cfg["kme"]["url"])
    env.setdefault("QKDL_URL", global_cfg["qkdl"]["url"])

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            f"{node_cfg['module']}:app",
            "--host", "0.0.0.0",
            "--port", str(node_cfg["port"]),
            "--log-level", "info",
        ],
        env=env,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    threading.Thread(
        target=_stream_output, args=(proc, label, log_path),
        daemon=True, name=f"log-{label}",
    ).start()
    print(f"Launching {label} from {node_cfg['module']}")
    print(f"  Started {label:<20} (port {node_cfg['port']}) pid={proc.pid}")
    return proc, log_path


#QKDL port extraction 

def _qkdl_port(node_cfg: dict, global_cfg: dict) -> int:
    url = node_cfg.get("env", {}).get("QKDL_URL", global_cfg["qkdl"]["url"])
    try:
        return int(url.rstrip("/").split(":")[-1])
    except (ValueError, IndexError):
        return 8003


#Main 

def main():
    parser = argparse.ArgumentParser(description="QKD Node Runner v0.8")
    parser.add_argument("--node",    help="Start only this node label")
    parser.add_argument("--list",    action="store_true", help="List nodes and exit")
    parser.add_argument("--no-qkdl", action="store_true",
                        help="Skip auto-launching QKDL instances")
    args = parser.parse_args()

    cfg   = load_config()
    nodes = cfg.get("nodes", [])

    if args.list:
        print("\nConfigured nodes:")
        for n in nodes:
            print(
                f"  {n['label']:<15} role={n['role']:<10} "
                f"port={n['port']}  qkdl_port={_qkdl_port(n, cfg)}"
            )
        return

    if args.node:
        nodes = [n for n in nodes if n["label"] == args.node]
        if not nodes:
            print(f"Node '{args.node}' not found in network.yaml")
            sys.exit(1)

    procs:  list[subprocess.Popen] = []
    labels: list[str]              = []
    logs:   list[Path]             = []

    def shutdown(sig, frame):
        print("\n[runner] Stopping all processes...")
        for p in procs:
            p.terminate()
        time.sleep(0.5)
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    #Launch QKDL instances 
    if not args.no_qkdl:
        qkdl_ports = sorted({_qkdl_port(n, cfg) for n in nodes})
        print(f"\nStarting {len(qkdl_ports)} QKDL instance(s)...\n")
        for port in qkdl_ports:
            proc, log = start_qkdl(port)
            procs.append(proc)
            labels.append(f"qkdl-{port}")
            logs.append(log)
        print(f"\n  Waiting 3s for QKDL instances to bind...\n")
        time.sleep(3.0)

    #Launch nodes 
    print(f"Starting {len(nodes)} node(s)...\n")
    for node_cfg in nodes:
        proc, log = start_node(node_cfg, cfg)
        procs.append(proc)
        labels.append(node_cfg["label"])
        logs.append(log)
        time.sleep(0.5)

    print(f"\n[runner] All processes started. Logs → {LOG_DIR}/  Ctrl+C to stop.\n")

    #Watch for unexpected exits 
    while True:
        for i, proc in enumerate(procs):
            ret = proc.poll()
            if ret is not None:
                label = labels[i]
                print(f"\n[runner] *** '{label}' exited with code {ret} ***")
                tail = _tail(logs[i])
                if tail:
                    print(f"[runner] Last lines from {label}:")
                    for line in tail:
                        print(f"  {line}")
                procs[i]  = subprocess.Popen(["true"])
                labels[i] = f"{label}(dead)"
        time.sleep(2)


if __name__ == "__main__":
    main()