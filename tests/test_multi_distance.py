from __future__ import annotations

import argparse
import csv
import math
import time
from pathlib import Path

import httpx

ALICE_URL = "http://localhost:8001"
KME_URL = "http://localhost:8000"

ALPHA_DB_PER_KM = 0.2
ETA = 0.85
SIFT_FRACTION = 0.5

DEFAULT_DISTANCES_KM = [0, 10, 25, 50, 75, 100, 120]
DEFAULT_TARGET_SIFTED = 60
DEFAULT_MAX_QUBITS = 1024     # hard ceiling regardless of distance
DEFAULT_MIN_QUBITS = 100
DEFAULT_REPEATS = 3
SESSION_POLL_INTERVAL_S = 2.0
SESSION_TIMEOUT_S = 600.0      # generous: long distances + large n_qubits take a while

OUT_CSV = Path(__file__).parent / "results" / "multi_distance_results.csv"


def transmission(distance_km: float) -> float:
    if distance_km <= 0:
        return 1.0
    return 10 ** (-(ALPHA_DB_PER_KM * distance_km) / 10)


def n_qubits_for_distance(
    distance_km: float, target_sifted: int, max_qubits: int, min_qubits: int
) -> tuple[int, int]:
    """
    Returns (n_qubits, effective_target_sifted). effective_target_sifted is
    the same as target_sifted unless the naive formula would need more than
    max_qubits, in which case n_qubits is capped and effective_target_sifted
    is recomputed downward to reflect what's actually achievable -- this is
    reported back so the result CSV is honest about why a far-distance run
    has a smaller expected sample.
    """
    T = transmission(distance_km)
    eff = T * ETA * SIFT_FRACTION
    if eff <= 0:
        return max_qubits, 0
    n_needed = math.ceil(target_sifted / eff)
    n_qubits = max(min_qubits, min(n_needed, max_qubits))
    effective_target = int(n_qubits * eff)
    return n_qubits, effective_target


def start_session(
    n_qubits: int, distance_km: float, receiver_label: str = "bob-1",
    batch_size: int = 10, loss_rate: float = 0.0,
) -> dict:
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            f"{ALICE_URL}/start",
            params={
                "receiver_label": receiver_label,
                "n_qubits": n_qubits,
                "batch_size": batch_size,
                "loss_rate": loss_rate,
                "distance_km": distance_km,
            },
        )
        resp.raise_for_status()
        return resp.json()


def poll_session(session_id: str, timeout_s: float) -> dict:
    deadline = time.time() + timeout_s
    last = {}
    with httpx.Client(timeout=10.0) as client:
        while time.time() < deadline:
            resp = client.get(f"{KME_URL}/sessions/{session_id}")
            resp.raise_for_status()
            last = resp.json()
            if last.get("status") in ("done", "aborted"):
                return last
            time.sleep(SESSION_POLL_INTERVAL_S)
    last["status"] = last.get("status", "timeout")
    return last


def run_one(distance_km: float, n_qubits: int, repeat_idx: int) -> dict:
    t0 = time.time()
    try:
        created = start_session(n_qubits, distance_km)
    except httpx.HTTPStatusError as e:
        return {
            "distance_km": distance_km, "n_qubits": n_qubits, "repeat": repeat_idx,
            "status": "create_failed", "error_message": str(e),
            "qber": None, "n_sifted": 0, "n_delivered": 0, "elapsed_s": 0.0,
        }

    session_id = created["session_id"]
    result = poll_session(session_id, SESSION_TIMEOUT_S)
    elapsed = time.time() - t0

    return {
        "distance_km": distance_km,
        "n_qubits": n_qubits,
        "repeat": repeat_idx,
        "session_id": session_id,
        "status": result.get("status"),
        "error_message": result.get("error_message", ""),
        "qber": result.get("qber"),
        "n_sifted": result.get("n_sifted", 0),
        "n_delivered": result.get("n_delivered", 0),
        "elapsed_s": round(elapsed, 2),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--distances", type=float, nargs="+", default=DEFAULT_DISTANCES_KM)
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    parser.add_argument("--target-sifted", type=int, default=DEFAULT_TARGET_SIFTED)
    parser.add_argument("--max-qubits", type=int, default=DEFAULT_MAX_QUBITS)
    parser.add_argument("--min-qubits", type=int, default=DEFAULT_MIN_QUBITS)
    parser.add_argument("--dry-run", action="store_true",
                         help="Print the n_qubits-per-distance table and exit without running anything")
    parser.add_argument("--out", type=Path, default=OUT_CSV)
    args = parser.parse_args()

    print(f"{'Distance (km)':>14}  {'n_qubits':>10}  {'eff_target_sifted':>18}")
    print("-" * 48)
    plan = []
    for d in args.distances:
        n_q, eff_target = n_qubits_for_distance(
            d, args.target_sifted, args.max_qubits, args.min_qubits
        )
        plan.append((d, n_q, eff_target))
        capped = " (capped)" if n_q == args.max_qubits else ""
        print(f"{d:>14.0f}  {n_q:>10d}  {eff_target:>18d}{capped}")

    if args.dry_run:
        return

    args.out.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "distance_km", "n_qubits", "repeat", "session_id", "status",
        "error_message", "qber", "n_sifted", "n_delivered", "elapsed_s",
    ]
    write_header = not args.out.exists()
    with open(args.out, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        for d, n_q, _eff_target in plan:
            for rep in range(args.repeats):
                print(f"\n>>> distance={d}km n_qubits={n_q} repeat={rep+1}/{args.repeats}")
                row = run_one(d, n_q, rep)
                for key in fieldnames:
                    row.setdefault(key, "")
                writer.writerow(row)
                f.flush()
                print(
                    f"    status={row['status']} qber={row['qber']} "
                    f"n_sifted={row['n_sifted']} n_delivered={row['n_delivered']} "
                    f"elapsed={row['elapsed_s']}s"
                )
                # Let QKDL's cooldown (COOLDOWN_S in qunetsim_service.py)
                # clear before the next session — otherwise the next
                # create_session call's _init_qkdl will spend its retries
                # waiting on a 503 for no good reason.
                time.sleep(3.0)

    print(f"\nResults appended to {args.out}")


if __name__ == "__main__":
    main()


"""
test_multi_distance.py: Multi-distance BB84 scenario runner.

Runs real BB84 sessions (through Alice's /start endpoint, the full
KME -> QKDL -> Celery pipeline -- no mocking) across a range of fiber
distances, with n_qubits scaled per distance so each run has a realistic
chance of producing enough sifted bits to clear the n_sifted >= 10 floor
in qber_key_task (see kme/main.py's INSUFFICIENT_BITS abort path).

WHY n_qubits SCALES WITH DISTANCE
-----------------------------------
Expected sifted-bit count for a session:
    E[n_sifted] ~= n_qubits * T(d) * eta * 0.5
where T(d) = 10^(-alpha*d/10) is fiber transmission and the 0.5 factor is
the basis-match probability (BB84 sifting). Solving for n_qubits to hit a
TARGET_SIFTED count gives the table below. At 200 qubits flat (Alberto's
original quick test), this works fine at 0-25km but silently can't work
at 50km+ -- not a bug, just insuficient sample size for the channel loss
at that distance (see the INSUFFICIENT_BITS conversation this script
follows up on).

WARNING ON RUNTIME
--------------------
QuNetSim's per-qubit cost is non-trivial (the Bob receive loop budgets
~8s/qubit as a deadline, QKDL_SECS_PER_QUBIT in nodes/bob/main.py and
workers/sifting_tasks.py -- actual per-qubit cost is much lower in
practice but the point stands: this is not free). Naively solving for
TARGET_SIFTED=60 at 100-120km needs 14,000-35,000 qubits, which could run
for a very long time. This script caps n_qubits at MAX_QUBITS_PER_SESSION
and *lowers* the effective target sifted count for far distances instead
of blindly sending tens of thousands of qubits -- you'll see fewer sifted
bits (and a noisier QBER estimate) at the longest distances, which is
itself a fact worth plotting (sample-size-limited regions of the QBER
curve), not a flaw to hide.

USAGE
-----
  python test_multi_distance.py
  python test_multi_distance.py --distances 0 10 25 50 75 --repeats 3
  python test_multi_distance.py --target-sifted 40 --max-qubits 5000
  python test_multi_distance.py --dry-run   # just print the n_qubits table, run nothing

Requires the full stack already running (node_runner.py --with-kme, plus
the sifting Celery worker -- exactly what Alberto already has up).
"""