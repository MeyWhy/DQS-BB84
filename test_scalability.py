"""
BB84 QKD Scalability Test
=========================
Runs sessions for each n_qubits value across your node pairs and writes
results to a CSV file ready for plotting.

Usage:
  python test_scalability.py                   # all pairs x all n_qubits
  python test_scalability.py --pairs 1         # only pair 1
  python test_scalability.py --qubits 64 128   # only those qubit counts
  python test_scalability.py --out my_run.csv  # custom output file
"""

import argparse
import csv
import time
import httpx
from datetime import datetime
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration — mirrors nodes.yaml
# ---------------------------------------------------------------------------

PAIRS = [
    {"alice_label": "alice-1", "bob_label": "bob-1", "alice_url": "http://localhost:8001"},
    {"alice_label": "alice-2", "bob_label": "bob-2", "alice_url": "http://localhost:8102"},
    {"alice_label": "alice-3", "bob_label": "bob-3", "alice_url": "http://localhost:8103"},
    {"alice_label": "alice-4", "bob_label": "bob-4", "alice_url": "http://localhost:8104"},
    {"alice_label": "alice-5", "bob_label": "bob-5", "alice_url": "http://localhost:8105"},
    {"alice_label": "alice-6", "bob_label": "bob-6", "alice_url": "http://localhost:8106"},
    {"alice_label": "alice-7", "bob_label": "bob-7", "alice_url": "http://localhost:8107"},
    {"alice_label": "alice-8", "bob_label": "bob-8", "alice_url": "http://localhost:8108"},
]

KME_URL="http://localhost:8000"


DEFAULT_QUBIT_COUNTS = [32, 64, 128,200, 1024, 2048]

POLL_INTERVAL   = 2.0   # seconds between status polls
SESSION_TIMEOUT = 600   # max wait per session (seconds)
CSV_FIELDS = [
    "session_id",
    "status",
    "alice",
    "bob",
    "n_qubits",
    "n_delivered",
    "n_sifted",
    "qber",
    "key_final",
    "key_status",
    "error_message",
    "elapsed_s",
    "wall_s",
    "progress_pct",
    "phase_label",
    "qkdl_url",
    "interceptor_label",
    "intercepted",
]


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def start_session(alice_url: str, bob_label: str, n_qubits: int,
                  interceptor_label: Optional[str] = None) -> str:
    """POST /start on Alice and return the session_id."""
    params: dict = {"receiver_label": bob_label, "n_qubits": n_qubits}
    if interceptor_label:
        params["interceptor_label"] = interceptor_label

    resp = httpx.post(f"{alice_url}/start", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()["session_id"]


def poll_session(session_id: str) -> dict:
    """Poll GET /session/{session_id} until done/error or timeout."""
    deadline = time.monotonic() + SESSION_TIMEOUT
    while time.monotonic() < deadline:
        resp = httpx.get(f"{KME_URL}/sessions/{session_id}", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") in ("done", "error"):
            return data
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Session {session_id} did not finish within {SESSION_TIMEOUT}s")


def run_one(pair: dict, n_qubits: int, run_at: str) -> dict:
    """Run a single session and return a CSV-ready record."""
    alice_url   = pair["alice_url"]
    alice_label = pair["alice_label"]
    bob_label   = pair["bob_label"]

    print(f"  → {alice_label} ↔ {bob_label}  n_qubits={n_qubits:>5} … ", end="", flush=True)
    t0 = time.monotonic()

    try:
        session_id = start_session(alice_url, bob_label, n_qubits)
        result     = poll_session(session_id)
        wall_s     = time.monotonic() - t0

        n_sifted    = result.get("n_sifted", 0)
        sift_ratio  = round(n_sifted / n_qubits, 4) if n_qubits else 0
        key_len     = len(result.get("key_final", ""))
        qber        = result.get("qber", "")
        status      = result.get("status", "?")

        n_delivered = result.get("n_delivered", 0)

        print(
            f"status={status}  delivered={n_delivered}  "
            f"sift={sift_ratio:.2%}  QBER={qber}  "
            f"wall={wall_s:.1f}s"
        )

        return {
            "session_id":        result.get("session_id", session_id),
            "status":            status,
            "alice":             alice_label,
            "bob":               bob_label,
            "n_qubits":          n_qubits,
            "n_delivered":       n_delivered,
            "n_sifted":          n_sifted,
            "qber":              qber,
            "key_final":         result.get("key_final", ""),
            "key_status":        result.get("key_status", ""),
            "error_message":     result.get("error_message", ""),
            "elapsed_s":         result.get("elapsed_s", ""),
            "wall_s":            round(wall_s, 2),   # keep wall_s as requested
            "progress_pct":      result.get("progress_pct", 100),
            "phase_label":       result.get("phase_label", "Key generated"),
            "qkdl_url":          result.get("qkdl_url", alice_url),
            "interceptor_label": result.get("interceptor_label"),
            "intercepted":       result.get("intercepted", False),
        }
    except Exception as exc:
        wall_s = time.monotonic() - t0
        print(f"FAILED  ({exc})")
        return {
            "session_id":        "",
            "status":            "aborted",
            "alice":             alice_label,
            "bob":               bob_label,
            "n_qubits":          n_qubits,
            "n_delivered":       0,
            "n_sifted":          0,
            "qber":              "",
            "key_final":         "",
            "key_status":        "",
            "error_message":     str(exc),
            "elapsed_s":         "",
            "wall_s":            round(wall_s, 2),
            "progress_pct":      0,
            "phase_label":       "Aborted",
            "qkdl_url":          alice_url,
            "interceptor_label": None,
            "intercepted":       False,
        }


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def save_csv(records: list[dict], path: str) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            # Fill missing keys with empty string so DictWriter doesn't complain
            writer.writerow({k: r.get(k, "") for k in CSV_FIELDS})
    print(f"\nResults saved → {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="BB84 QKD scalability test")
    parser.add_argument("--pairs",  nargs="+", type=int, metavar="N",
                        help="1-based pair numbers to test (default: all)")
    parser.add_argument("--qubits", nargs="+", type=int, metavar="N",
                        help=f"Qubit counts to test (default: {DEFAULT_QUBIT_COUNTS})")
    parser.add_argument("--out",    default="scalability_results.csv",
                        help="Output CSV file (default: scalability_results.csv)")
    args = parser.parse_args()

    pairs  = [PAIRS[i - 1] for i in args.pairs] if args.pairs else PAIRS
    qubits = args.qubits or DEFAULT_QUBIT_COUNTS
    run_at = datetime.now().isoformat(timespec="seconds")

    print(f"\nBB84 Scalability Test — {run_at}")
    print(f"Pairs  : {[p['alice_label'] for p in pairs]}")
    print(f"Qubits : {qubits}")
    print(f"Sessions: {len(qubits)}  (round-robin across {len(pairs)} pair(s))\n")

    records: list[dict] = []

    # Round-robin across pairs so no single QKDL is hit twice in a row.
    for i, n_qubits in enumerate(qubits):
        pair = pairs[i % len(pairs)]
        print(f"[{i + 1}/{len(qubits)}] n_qubits={n_qubits}")
        records.append(run_one(pair, n_qubits, run_at))

    save_csv(records, args.out)


if __name__ == "__main__":
    main()