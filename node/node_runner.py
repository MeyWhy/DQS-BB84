"""
node/node_runner.py
====================
Reads network.yaml and starts all configured nodes as subprocesses.
Adding a new node to network.yaml is the only step required.

Usage:
    python -m node.node_runner                    # start all nodes
    python -m node.node_runner --node alice-1     # start one node
    python -m node.node_runner --list             # list configured nodes
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "nodes" / "network.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def start_node(node_cfg: dict) -> subprocess.Popen:
    """
    Starts a single node as a subprocess.
    Sets environment variables from config before launch.
    """
    env = {**os.environ}
    env.update(node_cfg.get("env", {}))

    # Always pass KME and QKDL URLs from top-level config
    cfg = load_config()
    env.setdefault("KME_URL",  cfg["kme"]["url"])
    env.setdefault("QKDL_URL", cfg["qkdl"]["url"])

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
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
    )
    print(f"  Started {label:<15} (port {port}) pid={proc.pid}")
    return proc


def main():
    parser = argparse.ArgumentParser(description="QKD Node Runner")
    parser.add_argument("--node", help="Start only this node label")
    parser.add_argument("--list", action="store_true", help="List nodes")
    args = parser.parse_args()

    cfg   = load_config()
    nodes = cfg.get("nodes", [])

    if args.list:
        print("\nConfigured nodes:")
        for n in nodes:
            print(f"  {n['label']:<15} role={n['role']:<10} port={n['port']}")
        return

    if args.node:
        nodes = [n for n in nodes if n["label"] == args.node]
        if not nodes:
            print(f"Node '{args.node}' not found in network.yaml")
            sys.exit(1)

    print(f"\nStarting {len(nodes)} node(s)...\n")
    procs: list[subprocess.Popen] = []

    def shutdown(sig, frame):
        print("\nStopping nodes...")
        for p in procs:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for node_cfg in nodes:
        proc = start_node(node_cfg)
        procs.append(proc)
        time.sleep(0.5)   # stagger startup

    print(f"\nAll nodes started. Ctrl+C to stop.\n")

    # Wait for any node to exit unexpectedly
    while True:
        for i, proc in enumerate(procs):
            ret = proc.poll()
            if ret is not None:
                label = nodes[i]["label"]
                print(f"\nNode '{label}' exited with code {ret}")
        time.sleep(2)


if __name__ == "__main__":
    main()
