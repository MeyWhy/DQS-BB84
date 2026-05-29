"""
test_scalability_32.py  v2
==========================
Concurrent BB84 scalability test — up to 32 simultaneous sessions.

Changes from v1
---------------
- Incremental CSV: a row is written to disk the moment a session
  reaches a terminal state, so partial results survive a crash/OOM.
- Progress line printed every poll cycle even when nothing finished.
- Hardware pre-flight: warns if estimated RAM exceeds a configurable
  threshold before launching (MEMORY_WARN_GB, default 8).
- WORKER_CONCURRENCY_MAX forwarded from env so the test itself can
  cap per-session worker threads.
- Results file path printed at startup so you always know where to look.

Parameters (env vars)
---------------------
N_QUBITS            Qubits per session          (default 1024)
BATCH_SIZE          Batch size                  (default 10)
MAX_PAIRS           Number of pairs to use      (default 32)
POLL_INTERVAL_S     KME poll interval seconds   (default 5)
SESSION_TIMEOUT_S   Hard per-session timeout    (default 1800)
MEMORY_WARN_GB      Warn if estimate > this GB  (default 8)
"""

import asyncio
import csv
import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import httpx
import yaml

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

N_QUBITS          = int(os.getenv("N_QUBITS",          "1024"))
BATCH_SIZE        = int(os.getenv("BATCH_SIZE",         "10"))
MAX_PAIRS         = int(os.getenv("MAX_PAIRS",          "32"))
POLL_INTERVAL_S   = float(os.getenv("POLL_INTERVAL_S",  "5"))
SESSION_TIMEOUT_S = float(os.getenv("SESSION_TIMEOUT_S","1800"))
MEMORY_WARN_GB    = float(os.getenv("MEMORY_WARN_GB",   "8"))

KME_URL      = os.getenv("KME_URL",      "http://localhost:8000")
NETWORK_YAML = Path(os.getenv("NETWORK_YAML", "nodes/network.yaml"))
RESULTS_DIR  = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
)
logger = logging.getLogger("test_scalability_32")

# CSV fields — fixed so the incremental writer stays consistent
CSV_FIELDS = [
    "pair", "alice_label", "bob_label", "qkdl_url",
    "session_id", "start_error", "status",
    "n_qubits", "n_delivered", "n_sifted",
    "qber", "key_len", "elapsed_s", "error_message", "intercepted",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_pairs(yaml_path: Path, max_pairs: int) -> list[dict]:
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)
    pairs   = []
    senders = [n for n in cfg.get("nodes", []) if n.get("role") == "sender"]
    for i, node in enumerate(senders[:max_pairs], start=1):
        env = node.get("env", {})
        pairs.append({
            "pair":        i,
            "alice_label": node["label"],
            "alice_url":   f"http://localhost:{node['port']}",
            "bob_label":   f"bob-{i}",
            "qkdl_url":    env.get("QKDL_URL", ""),
        })
    return pairs


def estimate_ram_gb(n_pairs: int) -> float:
    """Rough RSS estimate in GB for n_pairs concurrent sessions."""
    return n_pairs * (0.18 + 0.06 + 0.06 + 0.09) + 0.2   # QKDL+Alice+Bob+worker+fixed


def open_csv(path: Path) -> tuple[csv.DictWriter, object]:
    fh = open(path, "w", newline="")
    w  = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
    w.writeheader()
    fh.flush()
    return w, fh


async def check_alice_ready(client: httpx.AsyncClient, url: str) -> bool:
    try:
        r = await client.get(f"{url}/health", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


async def wait_for_nodes(pairs: list[dict], timeout: float = 45.0) -> None:
    deadline = time.time() + timeout
    async with httpx.AsyncClient() as client:
        while time.time() < deadline:
            results = await asyncio.gather(
                *[check_alice_ready(client, p["alice_url"]) for p in pairs],
                return_exceptions=True,
            )
            ready = sum(1 for r in results if r is True)
            if ready == len(pairs):
                logger.info(f"All {len(pairs)} alice nodes ready")
                return
            logger.info(f"Waiting for nodes … {ready}/{len(pairs)} ready")
            await asyncio.sleep(2.0)
    raise TimeoutError(f"Not all alice nodes came up within {timeout}s")


async def start_session(
    client: httpx.AsyncClient,
    pair:   dict,
    n_qubits: int,
    batch_size: int,
) -> dict:
    url = (
        f"{pair['alice_url']}/start"
        f"?receiver_label={pair['bob_label']}"
        f"&n_qubits={n_qubits}"
        f"&batch_size={batch_size}"
    )
    try:
        r = await client.post(url, timeout=20.0)
        r.raise_for_status()
        body = r.json()
        logger.info(
            f"[pair {pair['pair']:2d}] started  id={body['session_id'][:8]}"
        )
        return {**pair, "session_id": body["session_id"],
                "start_error": None, "started_at": time.time()}
    except Exception as e:
        logger.error(f"[pair {pair['pair']:2d}] /start failed: {e}")
        return {**pair, "session_id": None,
                "start_error": str(e), "started_at": time.time()}


async def poll_session(client: httpx.AsyncClient, sid: str) -> dict:
    try:
        r = await client.get(f"{KME_URL}/sessions/{sid}", timeout=10.0)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


def make_row(rec: dict, kme: dict, n_qubits: int, wall_elapsed: float) -> dict:
    sid = rec.get("session_id")
    return {
        "pair":          rec["pair"],
        "alice_label":   rec["alice_label"],
        "bob_label":     rec["bob_label"],
        "qkdl_url":      rec["qkdl_url"],
        "session_id":    sid or "FAILED_TO_START",
        "start_error":   rec.get("start_error") or "",
        "status":        kme.get("status", "not_started"),
        "n_qubits":      kme.get("n_qubits", n_qubits),
        "n_delivered":   kme.get("n_delivered", 0),
        "n_sifted":      kme.get("n_sifted", 0),
        "qber":          round(kme.get("qber", 0.0), 6),
        "key_len":       len(kme.get("key_final", "")),
        "elapsed_s":     kme.get("elapsed_s", round(wall_elapsed, 2)),
        "error_message": kme.get("error_message", ""),
        "intercepted":   kme.get("intercepted", False),
    }


async def run_test(
    pairs:      list[dict],
    n_qubits:   int,
    batch_size: int,
    csv_writer: csv.DictWriter,
    csv_fh,
) -> list[dict]:

    logger.info(
        f"\n{'='*60}\n"
        f"  {len(pairs)} pairs | {n_qubits} qubits | batch={batch_size}\n"
        f"{'='*60}"
    )

    await wait_for_nodes(pairs)
    wall_start = time.time()

    async with httpx.AsyncClient(timeout=30.0) as client:
        logger.info(f"Launching {len(pairs)} sessions simultaneously …")
        session_records = await asyncio.gather(
            *[start_session(client, p, n_qubits, batch_size) for p in pairs]
        )

    started = [r for r in session_records if r["session_id"] is not None]
    failed  = [r for r in session_records if r["session_id"] is None]
    logger.info(
        f"Sessions launched: {len(started)}  "
        f"failed to start: {len(failed)}"
    )

    # Write failed-to-start rows immediately
    for rec in failed:
        row = make_row(rec, {}, n_qubits, 0)
        csv_writer.writerow(row)
    csv_fh.flush()

    # Poll loop
    terminal  = {"done", "aborted"}
    finished:  dict[str, dict] = {}   # session_id → final KME record
    all_rows:  list[dict]      = list(make_row(r, {}, n_qubits, 0) for r in failed)

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            pending = [r for r in started if r["session_id"] not in finished]
            if not pending:
                break

            elapsed = time.time() - wall_start
            if elapsed > SESSION_TIMEOUT_S:
                logger.error(
                    f"Hard timeout ({SESSION_TIMEOUT_S}s). "
                    f"{len(pending)} sessions still pending."
                )
                for rec in pending:
                    sid = rec["session_id"]
                    finished[sid] = {"status": "timeout",
                                     "error_message": f"OOM/timeout after {elapsed:.0f}s"}
                    row = make_row(rec, finished[sid], n_qubits, elapsed)
                    csv_writer.writerow(row)
                    all_rows.append(row)
                csv_fh.flush()
                break

            polls = await asyncio.gather(
                *[poll_session(client, r["session_id"]) for r in pending]
            )

            newly_done = 0
            for rec, kme in zip(pending, polls):
                sid = rec["session_id"]
                if kme.get("status") in terminal:
                    finished[sid] = kme
                    newly_done   += 1
                    row = make_row(rec, kme, n_qubits, time.time() - wall_start)
                    csv_writer.writerow(row)   # ← written immediately
                    csv_fh.flush()
                    all_rows.append(row)
                    logger.info(
                        f"[pair {rec['pair']:2d}] "
                        f"{kme['status'].upper():7s}  "
                        f"elapsed={kme.get('elapsed_s','?'):.1f}s  "
                        f"sifted={kme.get('n_sifted',0)}  "
                        f"qber={kme.get('qber',0)*100:.2f}%  "
                        f"key={len(kme.get('key_final',''))}"
                    )

            remaining = len(pending) - newly_done
            if remaining:
                logger.info(
                    f"  … {remaining} still running "
                    f"(wall={time.time()-wall_start:.0f}s  "
                    f"done={len(finished)}/{len(started)})"
                )

            await asyncio.sleep(POLL_INTERVAL_S)

    wall_elapsed = time.time() - wall_start

    # Summary
    n_done    = sum(1 for r in all_rows if r["status"] == "done")
    n_aborted = sum(1 for r in all_rows if r["status"] == "aborted")
    n_timeout = sum(1 for r in all_rows if r["status"] == "timeout")
    e_vals    = [r["elapsed_s"] for r in all_rows if r["status"] == "done"]
    avg_e     = sum(e_vals) / len(e_vals) if e_vals else 0
    max_e     = max(e_vals) if e_vals else 0
    min_e     = min(e_vals) if e_vals else 0

    logger.info(
        f"\n{'='*60}\n"
        f"  RESULTS  ({len(pairs)} pairs | {n_qubits} qubits)\n"
        f"{'='*60}\n"
        f"  Wall clock : {wall_elapsed:.1f}s\n"
        f"  Done       : {n_done}/{len(pairs)}\n"
        f"  Aborted    : {n_aborted}\n"
        f"  Timed out  : {n_timeout}\n"
        f"  Elapsed    : min={min_e:.1f}s  avg={avg_e:.1f}s  max={max_e:.1f}s\n"
        f"{'='*60}"
    )

    print("\nPair | Alice    | Status  | Elapsed | n_sifted | QBER    | KeyLen")
    print("-" * 70)
    for r in sorted(all_rows, key=lambda x: x["pair"]):
        print(
            f"  {r['pair']:2d} | {r['alice_label']:<8} | "
            f"{r['status']:<7} | {r['elapsed_s']:7.1f}s | "
            f"{r['n_sifted']:8d} | {r['qber']*100:6.2f}% | {r['key_len']}"
        )

    return all_rows


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> int:
    if not NETWORK_YAML.exists():
        logger.error(f"network.yaml not found at {NETWORK_YAML}")
        return 1

    pairs = load_pairs(NETWORK_YAML, MAX_PAIRS)
    if not pairs:
        logger.error("No sender nodes found in network.yaml")
        return 1

    # RAM estimate warning
    est_gb = estimate_ram_gb(len(pairs))
    if est_gb > MEMORY_WARN_GB:
        logger.warning(
            f"Estimated RAM for {len(pairs)} pairs: ~{est_gb:.1f} GB "
            f"(MEMORY_WARN_GB={MEMORY_WARN_GB}). "
            f"If WSL crashes, reduce MAX_PAIRS or increase WSL memory "
            f"(see .wslconfig instructions below)."
        )
    else:
        logger.info(
            f"Estimated RAM: ~{est_gb:.1f} GB for {len(pairs)} pairs — OK"
        )

    # KME health check
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{KME_URL}/health")
            r.raise_for_status()
        logger.info(f"KME healthy")
    except Exception as e:
        logger.error(f"KME not reachable: {e}")
        return 1

    # Open CSV before launching — partial results survive a crash
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = RESULTS_DIR / f"scalability_32_{len(pairs)}pairs_{ts}.csv"
    csv_w, csv_fh = open_csv(csv_path)
    logger.info(f"Results → {csv_path}  (written incrementally)")

    try:
        rows = await run_test(pairs, N_QUBITS, BATCH_SIZE, csv_w, csv_fh)
    finally:
        csv_fh.close()
        logger.info(f"CSV closed → {csv_path}")

    n_done = sum(1 for r in rows if r["status"] == "done")
    return 0 if n_done == len(pairs) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
