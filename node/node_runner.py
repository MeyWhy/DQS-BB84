"""
node/node_runner.py  — v2 (scalability fix)

Changes vs v1:
- Reads optional `qkdl_port` from each node entry in network.yaml.
- Launches one qunetsim_service process per unique QKDL port referenced by
  the selected nodes, before starting the nodes themselves.
- This lets parallel alice/bob pairs run truly concurrently without fighting
  over the QuNetSim singleton.

Usage:
    python -m node.node_runner                     # start all nodes + QKDLs
    python -m node.node_runner --node alice-1      # start one node
    python -m node.node_runner --list              # list configured nodes
    python -m node.node_runner --no-qkdl           # skip auto-launching QKDLs
                                                   # (if you manage them yourself)
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import yaml

ROOT        = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "nodes" / "network.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def start_qkdl(port: int, default_qkdl_url: str) -> subprocess.Popen:
    """Launch a qunetsim_service instance on the given port."""
    env = {**os.environ, "QKDL_PORT": str(port)}

    cmd = [
        sys.executable, "-m", "uvicorn",
        "qunetsim_service:app",
        "--host", "0.0.0.0",
        "--port", str(port),
        "--log-level", "warning",
    ]

    log_path = ROOT / "logs" / f"qkdl-{port}.log"
    log_path.parent.mkdir(exist_ok=True)

    proc = subprocess.Popen(
        cmd,
        env=env,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    print(f"  Started QKDL              (port {port}) pid={proc.pid}")
    return proc


def start_node(node_cfg: dict, global_cfg: dict) -> subprocess.Popen:
    env = {**os.environ}
    env.update(node_cfg.get("env", {}))

    # Top-level KME URL always available as fallback
    env.setdefault("KME_URL",  global_cfg["kme"]["url"])
    # QKDL_URL: prefer node-level env (set in network.yaml), else global
    env.setdefault("QKDL_URL", global_cfg["qkdl"]["url"])

    module = node_cfg["module"]
    port   = node_cfg["port"]
    label  = node_cfg["label"]

    cmd = [
        sys.executable, "-m", "uvicorn",
        f"{module}:app",
        "--host", "0.0.0.0",
        "--port", str(port),
        "--log-level", "info",
    ]

    log_path = ROOT / "logs" / f"{label}.log"
    log_path.parent.mkdir(exist_ok=True)

    proc = subprocess.Popen(
        cmd,
        env=env,
        cwd=str(ROOT),
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    print(f"Launching {label} from {module}")
    print(f"  Started {label:<15} (port {port}) pid={proc.pid}")
    return proc


def _extract_qkdl_port(node_cfg: dict, global_cfg: dict) -> int:
    """Get the QKDL port this node will use."""
    # Check node-level env override first
    qkdl_url = node_cfg.get("env", {}).get("QKDL_URL", "")
    if not qkdl_url:
        qkdl_url = global_cfg["qkdl"]["url"]
    # Parse port from URL
    try:
        return int(qkdl_url.rstrip("/").split(":")[-1])
    except (ValueError, IndexError):
        return 8003


def main():
    parser = argparse.ArgumentParser(description="QKD Node Runner")
    parser.add_argument("--node",    help="Start only this node label")
    parser.add_argument("--list",    action="store_true", help="List nodes")
    parser.add_argument("--no-qkdl", action="store_true",
                        help="Don't auto-launch QKDL instances")
    args = parser.parse_args()

    cfg   = load_config()
    nodes = cfg.get("nodes", [])

    if args.list:
        print("\nConfigured nodes:")
        for n in nodes:
            qkdl_port = _extract_qkdl_port(n, cfg)
            print(
                f"  {n['label']:<15} role={n['role']:<10} "
                f"port={n['port']}  qkdl={qkdl_port}"
            )
        return

    if args.node:
        nodes = [n for n in nodes if n["label"] == args.node]
        if not nodes:
            print(f"Node '{args.node}' not found in network.yaml")
            sys.exit(1)

    # ── Collect unique QKDL ports needed by selected nodes ─────────────────
    qkdl_ports: set[int] = set()
    for node_cfg in nodes:
        qkdl_ports.add(_extract_qkdl_port(node_cfg, cfg))

    procs: list[subprocess.Popen] = []
    proc_labels: list[str] = []

    def shutdown(sig, frame):
        print("\nStopping all processes...")
        for p in procs:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── Launch QKDL instances first ─────────────────────────────────────────
    if not args.no_qkdl:
        print(f"\nStarting {len(qkdl_ports)} QKDL instance(s)...\n")
        for port in sorted(qkdl_ports):
            proc = start_qkdl(port, cfg["qkdl"]["url"])
            procs.append(proc)
            proc_labels.append(f"qkdl-{port}")
        # Give QKDLs a moment to bind their ports before nodes try to connect
        time.sleep(2.0)

    # ── Launch nodes ────────────────────────────────────────────────────────
    print(f"\nStarting {len(nodes)} node(s)...\n")
    for node_cfg in nodes:
        proc = start_node(node_cfg, cfg)
        procs.append(proc)
        proc_labels.append(node_cfg["label"])
        time.sleep(0.5)   # stagger startup

    print(f"\nAll processes started. Ctrl+C to stop.\n")

    # ── Watch for unexpected exits ──────────────────────────────────────────
    while True:
        for i, proc in enumerate(procs):
            ret = proc.poll()
            if ret is not None:
                label = proc_labels[i]
                print(f"\nProcess '{label}' exited with code {ret}")
        time.sleep(2)


if __name__ == "__main__":
    main()