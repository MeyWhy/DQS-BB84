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


def _stream_output(proc: subprocess.Popen, label: str, log_path: Path) -> None:
    log_path.parent.mkdir(exist_ok=True)
    with open(log_path, "a", buffering=1) as fh:
        for raw in proc.stdout:
            line = raw.decode(errors="replace").rstrip()
            fh.write(line + "\n")
            print(f"[{label}] {line}", flush=True)


def _tail(log_path: Path, n: int = 20) -> list[str]:
    try:
        return log_path.read_text(errors="replace").splitlines()[-n:]
    except Exception:
        return []


def _qkdl_url(node_cfg: dict, global_cfg: dict) -> str:
    return node_cfg.get("env", {}).get("QKDL_URL", global_cfg["qkdl"]["url"])


def _qkdl_port(node_cfg: dict, global_cfg: dict) -> int:
    try:
        return int(_qkdl_url(node_cfg, global_cfg).rstrip("/").split(":")[-1])
    except (ValueError, IndexError):
        return 8003


def compute_qkdl_urls(nodes: list[dict], global_cfg: dict) -> str:
    """
    Collect every unique QKDL URL referenced by sender/receiver nodes
    (preserving order) and return as comma-separated string for QKDL_URLS.
    Monitor nodes (Eve) do not own a QKDL, so they are skipped here.
    """
    seen: dict[str, bool] = {}
    for n in nodes:
        if n.get("role") in ("monitor",):
            continue
        seen[_qkdl_url(n, global_cfg)] = True
    return ",".join(seen.keys())


def start_qkdl(port: int) -> tuple[subprocess.Popen, Path]:
    label    = f"qkdl-{port}"
    log_path = LOG_DIR / f"{label}.log"
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "qunetsim_service:app",
         "--host", "0.0.0.0", "--port", str(port), "--log-level", "warning"],
        env={**os.environ, "QKDL_PORT": str(port)},
        cwd=str(ROOT),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    threading.Thread(target=_stream_output, args=(proc, label, log_path),
                     daemon=True, name=f"log-{label}").start()
    print(f"  Started {label:<20} (port {port}) pid={proc.pid}")
    return proc, log_path


def start_kme(qkdl_urls: str, global_cfg: dict) -> tuple[subprocess.Popen, Path]:
    log_path = LOG_DIR / "kme.log"
    kme_port = int(global_cfg["kme"]["url"].rstrip("/").split(":")[-1])
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "kme.main:app",
         "--host", "0.0.0.0", "--port", str(kme_port), "--log-level", "info"],
        env={**os.environ, "QKDL_URLS": qkdl_urls},
        cwd=str(ROOT),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    threading.Thread(target=_stream_output, args=(proc, "kme", log_path),
                     daemon=True, name="log-kme").start()
    print(f"  Started kme                  (port {kme_port}) pid={proc.pid}")
    return proc, log_path


def start_node(node_cfg: dict, global_cfg: dict) -> tuple[subprocess.Popen, Path]:
    label    = node_cfg["label"]
    log_path = LOG_DIR / f"{label}.log"
    env = {**os.environ}
    env.update(node_cfg.get("env", {}))
    env.setdefault("KME_URL",  global_cfg["kme"]["url"])
    #Monitor nodes (Eve) have no QKDL; skip the default
    if node_cfg.get("role") != "monitor":
        env.setdefault("QKDL_URL", global_cfg["qkdl"]["url"])
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", f"{node_cfg['module']}:app",
         "--host", "0.0.0.0", "--port", str(node_cfg["port"]),
         "--log-level", "info"],
        env=env, cwd=str(ROOT),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    threading.Thread(target=_stream_output, args=(proc, label, log_path),
                     daemon=True, name=f"log-{label}").start()
    print(f"  Started {label:<20} (port {node_cfg['port']}) pid={proc.pid}")
    return proc, log_path


def _celery_launch_hint(qkdl_urls: str) -> None:
    print("\n[runner] Start Celery workers in separate terminals:")
    print("  #Qubit sender pool (scale --concurrency for more throughput):")
    print("  celery -A workers.celery_config worker \\")
    print("         --queues=qubit_send --concurrency=4 --loglevel=info -n qubit@%h")
    print("  #Sifting chord callback (1 worker is sufficient):")
    print("  celery -A workers.celery_config worker \\")
    print("         --queues=sifting --concurrency=1 --loglevel=info -n sifting@%h\n")


def main():
    parser = argparse.ArgumentParser(description="QKD Node Runner v0.9.0")
    parser.add_argument("--node",     help="Start only this node label")
    parser.add_argument("--list",     action="store_true")
    parser.add_argument("--no-qkdl",  action="store_true",
                        help="Skip auto-launching QKDL instances")
    parser.add_argument("--with-kme", action="store_true",
                        help="Also launch KME with correct QKDL_URLS injected")
    args = parser.parse_args()

    cfg   = load_config()
    nodes = cfg.get("nodes", [])

    if args.list:
        print("\nConfigured nodes:")
        for n in nodes:
            role = n["role"]
            qkdl = (f"  qkdl={_qkdl_url(n, cfg)}"
                    if role != "monitor" else "  (no QKDL)")
            print(f"  {n['label']:<15} role={role:<10} port={n['port']}{qkdl}")
        qkdl_urls = compute_qkdl_urls(nodes, cfg)
        print(f"\nKME launch command:")
        print(f"  QKDL_URLS={qkdl_urls} uvicorn kme.main:app --port 8000")
        _celery_launch_hint(qkdl_urls)
        return

    if args.node:
        nodes = [n for n in nodes if n["label"] == args.node]
        if not nodes:
            print(f"Node '{args.node}' not found in network.yaml")
            sys.exit(1)

    #Separate sender/receiver nodes (need QKDLs) from monitor nodes (don't)
    qkdl_nodes   = [n for n in nodes if n.get("role") != "monitor"]
    monitor_nodes = [n for n in nodes if n.get("role") == "monitor"]
    qkdl_urls    = compute_qkdl_urls(nodes, cfg)

    print(f"\n[runner] QKDL pool: {qkdl_urls}")

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

    #QKDLs (one per unique port referenced by sender/receiver nodes)
    if not args.no_qkdl:
        ports = sorted({_qkdl_port(n, cfg) for n in qkdl_nodes})
        if ports:
            print(f"\nStarting {len(ports)} QKDL instance(s)...\n")
            for port in ports:
                proc, log = start_qkdl(port)
                procs.append(proc); labels.append(f"qkdl-{port}"); logs.append(log)
            print(f"\n  Waiting 3s for QKDLs to bind...\n")
            time.sleep(3.0)

    #KME (optional)
    if args.with_kme:
        print(f"\nStarting KME...\n")
        proc, log = start_kme(qkdl_urls, cfg)
        procs.append(proc); labels.append("kme"); logs.append(log)
        print(f"\n  Waiting 2s for KME to bind...\n")
        time.sleep(2.0)
    else:
        print(f"[runner] Start KME separately with:")
        print(f"         QKDL_URLS={qkdl_urls} uvicorn kme.main:app --port 8000\n")

    #Celery reminder (runner does not launch workers — they need their own pool)
    _celery_launch_hint(qkdl_urls)

    #All nodes (sender, receiver, monitor)
    all_nodes = qkdl_nodes + monitor_nodes
    print(f"Starting {len(all_nodes)} node(s)...\n")
    for node_cfg in all_nodes:
        proc, log = start_node(node_cfg, cfg)
        procs.append(proc); labels.append(node_cfg["label"]); logs.append(log)
        time.sleep(0.5)

    print(f"\n[runner] All up. Logs → {LOG_DIR}/  Ctrl+C to stop.\n")

    while True:
        for i, proc in enumerate(procs):
            ret = proc.poll()
            if ret is not None:
                label = labels[i]
                print(f"\n[runner] *** '{label}' exited (code {ret}) ***")
                for line in _tail(logs[i]):
                    print(f"  {line}")
                procs[i]  = subprocess.Popen(["true"])
                labels[i] = f"{label}(dead)"
        time.sleep(2)


if __name__ == "__main__":
    main()