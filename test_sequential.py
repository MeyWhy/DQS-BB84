import argparse
import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

# ── Configuration ─────────────────────────────────────────────────────────────

ALICE_URLS = {
    "alice-1": os.getenv("ALICE_1_URL", "http://localhost:8001")
}
QKDL_URLS = [
    os.getenv("QKDL_1_URL", "http://localhost:8003")
]
KME_URL = os.getenv("KME_URL", "http://localhost:8000")

N_QUBITS_LIST = [32, 64, 128, 200, 1024, 2048]
BATCH_SIZES   = [10, 20]
ALICE_NODES   = ["alice-1"]
BOB_NODES     = ["bob-1"]

POLL_INTERVAL        = 2.0     # seconds between KME status polls
INTER_SESSION_DELAY  = 4.0     # seconds between sessions (> QKDL cooldown 2.5s)
MAX_WAIT_PER_SESSION = int(os.getenv("MAX_WAIT_S", "1200"))

# Minimum sifted bits that BB84 can reliably produce.
# With basis-match probability ~0.5, n_qubits=16 gives ~8 sifted → below threshold.
# Skip any combo where expected sifted < 10 + safety margin.
MIN_QUBITS_FOR_BATCH = {10: 20, 20: 40}  # batch_size -> min n_qubits

CSV_COLUMNS = [
    "run_index", "alice_label", "bob_label",
    "batch_size", "n_qubits",
    "status", "latency_s",
    "session_id", "n_delivered", "n_sifted", "qber",
    "key_final", "error_message",
    "progress_pct", "phase_label",
    "loss_rate", "interceptor_label", "retry_enabled",
    "qkdl_url", "intercepted",
    "run_type", "timestamp",
]


# ── Preflight ─────────────────────────────────────────────────────────────────

def preflight_check() -> bool:
    """Verify KME and all Alice nodes are reachable before starting."""
    print("\n[preflight] Checking services...")
    ok = True

    # KME
    try:
        r = httpx.get(f"{KME_URL}/health", timeout=5.0)
        r.raise_for_status()
        print(f"  KME       {KME_URL}  OK")
    except Exception as e:
        print(f"  KME       {KME_URL}  FAIL — {e}")
        ok = False

    # Alice nodes
    for label, url in ALICE_URLS.items():
        try:
            r = httpx.get(f"{url}/health", timeout=5.0)
            r.raise_for_status()
            print(f"  {label:<10} {url}  OK")
        except Exception as e:
            print(f"  {label:<10} {url}  FAIL — {e}")
            ok = False

    # QKDLs (warn but don't block — KME manages the pool)
    for url in QKDL_URLS:
        try:
            r = httpx.get(f"{url}/health", timeout=5.0)
            r.raise_for_status()
            print(f"  QKDL      {url}  OK")
        except Exception as e:
            print(f"  QKDL      {url}  WARN — {e}")

    if not ok:
        print("\n[preflight] FAILED — fix the above before running the test.\n")
    else:
        print("[preflight] All services reachable.\n")
    return ok


# ── Test matrix ───────────────────────────────────────────────────────────────

def is_valid_combo(n_qubits: int, batch_size: int) -> bool:
    """
    Skip combos guaranteed to produce INSUFFICIENT_BITS.
    Expected sifted ~= n_qubits * 0.5. Need >= 10 sifted after QBER sample removal.
    Conservatively: n_qubits >= 25 for any batch_size.
    For batch_size=20: n_qubits must be >= 40 (batch sends 20 at a time,
    single batch of only 16 qubits works but sifted is borderline).
    """
    min_n = MIN_QUBITS_FOR_BATCH.get(batch_size, 20)
    return n_qubits >= min_n


def build_matrix() -> list[dict]:
    cases = []
    idx   = 1
    skipped = 0
    for alice in ALICE_NODES:
        for bob in BOB_NODES:
            for n in N_QUBITS_LIST:
                for b in BATCH_SIZES:
                    if not is_valid_combo(n, b):
                        skipped += 1
                        continue
                    cases.append({
                        "run_index":   idx,
                        "alice_label": alice,
                        "bob_label":   bob,
                        "n_qubits":    n,
                        "batch_size":  b,
                    })
                    idx += 1
    return cases, skipped


# ── Session helpers ───────────────────────────────────────────────────────────

def start_session(alice_url: str, bob_label: str,
                  n_qubits: int, batch_size: int) -> dict:
    resp = httpx.post(
        f"{alice_url}/start",
        params={
            "receiver_label": bob_label,
            "n_qubits":       n_qubits,
            "batch_size":     batch_size,
            "loss_rate":      0.0,
            "retry_enabled":  False,
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()


def poll_until_done(session_id: str, max_wait: int) -> tuple[dict, bool]:
    """Returns (result_dict, timed_out)."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            r = httpx.get(f"{KME_URL}/sessions/{session_id}", timeout=10.0)
            r.raise_for_status()
            data = r.json()
            if data.get("status") in ("done", "aborted"):
                return data, False
        except Exception as e:
            print(f"    [poll] {e}")
        time.sleep(POLL_INTERVAL)

    # Timed out — fetch final state
    try:
        r    = httpx.get(f"{KME_URL}/sessions/{session_id}", timeout=10.0)
        data = r.json()
    except Exception:
        data = {"status": "timeout", "session_id": session_id}
    return data, True


def cleanup_after_timeout(session_id: str, qkdl_url: str) -> None:
    """
    After a timeout, cancel the KME session and reset the QKDL.
    This releases the QKDL pool lock and clears any zombie QuNetSim session,
    preventing 409 Conflict errors on subsequent sessions.
    """
    print(f"    [cleanup] Cancelling session {session_id[:8]} on KME...")
    try:
        httpx.delete(f"{KME_URL}/sessions/{session_id}", timeout=10.0)
    except Exception as e:
        print(f"    [cleanup] KME cancel failed: {e}")

    print(f"    [cleanup] Resetting QKDL {qkdl_url}...")
    try:
        httpx.post(f"{qkdl_url}/network/reset", timeout=10.0)
    except Exception as e:
        print(f"    [cleanup] QKDL reset failed: {e}")

    # Extra wait for QKDL to fully reset before next session
    time.sleep(3.0)


def session_to_row(case: dict, result: dict, timed_out: bool) -> dict:
    status = "timeout" if timed_out else result.get("status", "unknown")
    return {
        "run_index":         case["run_index"],
        "alice_label":       case["alice_label"],
        "bob_label":         case["bob_label"],
        "batch_size":        case["batch_size"],
        "n_qubits":          case["n_qubits"],
        "status":            status,
        "latency_s":         result.get("elapsed_s", ""),
        "session_id":        result.get("session_id", ""),
        "n_delivered":       result.get("n_delivered", ""),
        "n_sifted":          result.get("n_sifted", ""),
        "qber":              result.get("qber", ""),
        "key_final":         result.get("key_final", ""),
        "error_message":     result.get("error_message", ""),
        "progress_pct":      result.get("progress_pct", ""),
        "phase_label":       result.get("phase_label", ""),
        "loss_rate":         0.0,
        "interceptor_label": "",
        "retry_enabled":     False,
        "qkdl_url":          result.get("qkdl_url", ""),
        "intercepted":       result.get("intercepted", False),
        "run_type":          "sequential",
        "timestamp":         datetime.utcnow().isoformat(),
    }


def append_row(csv_path: Path, row: dict, write_header: bool) -> None:
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="BB84 sequential test suite")
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--skip-from",  type=int, default=0,
                        help="Skip rows with run_index < N (resume from N)")
    parser.add_argument("--output",     default="results/sequential_results.csv")
    args = parser.parse_args()

    csv_path = Path(args.output)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    matrix, n_skipped = build_matrix()
    total = len(matrix)

    print(f"\n{'='*65}")
    print(f"  BB84 Sequential Test Suite")
    print(f"  {total} sessions planned  ({n_skipped} skipped — insufficient qubits)")
    if args.skip_from:
        print(f"  Resuming from run_index >= {args.skip_from}")
    print(f"  Output: {csv_path}")
    print(f"  Max wait per session: {MAX_WAIT_PER_SESSION}s")
    print(f"  Inter-session delay:  {INTER_SESSION_DELAY}s")
    print(f"{'='*65}\n")

    if args.dry_run:
        print(f"{'IDX':>4}  {'ALICE':<10} {'BOB':<8} {'N_Q':>5} {'BATCH':>5}")
        print("-" * 42)
        for c in matrix:
            print(
                f"{c['run_index']:>4}  {c['alice_label']:<10} "
                f"{c['bob_label']:<8} {c['n_qubits']:>5} {c['batch_size']:>5}"
            )
        print(f"\n{total} sessions planned. Re-run without --dry-run to execute.")
        return

    if not preflight_check():
        sys.exit(1)

    # Filter for resume
    if args.skip_from:
        matrix = [c for c in matrix if c["run_index"] >= args.skip_from]
        print(f"[resume] Starting from run_index={matrix[0]['run_index']}\n")

    write_header = not csv_path.exists()
    passed = failed = 0
    t_suite_start = time.time()

    for i, case in enumerate(matrix):
        alice  = case["alice_label"]
        bob    = case["bob_label"]
        n      = case["n_qubits"]
        b      = case["batch_size"]
        a_url  = ALICE_URLS[alice]
        total_done = i + 1

        print(
            f"[{case['run_index']:>3}/{total}] {alice} → {bob}  "
            f"n_qubits={n:<5} batch={b}  ",
            end="", flush=True,
        )

        # Start session
        try:
            start_resp = start_session(a_url, bob, n, b)
            session_id = start_resp["session_id"]
            qkdl_url   = start_resp.get("qkdl_url", QKDL_URLS[0])
        except httpx.HTTPStatusError as e:
            print(f"SKIP — HTTP {e.response.status_code}: {e.response.text[:80]}")
            row = session_to_row(case, {
                "status": f"http_{e.response.status_code}",
                "error_message": e.response.text[:200],
            }, False)
            append_row(csv_path, row, write_header)
            write_header = False
            time.sleep(INTER_SESSION_DELAY)
            continue
        except Exception as e:
            print(f"ERROR — {e}")
            row = session_to_row(case, {
                "status":        "start_error",
                "error_message": str(e),
            }, False)
            append_row(csv_path, row, write_header)
            write_header = False
            time.sleep(INTER_SESSION_DELAY)
            continue

        # Poll until done
        result, timed_out = poll_until_done(session_id, MAX_WAIT_PER_SESSION)
        status  = "timeout" if timed_out else result.get("status", "unknown")
        latency = result.get("elapsed_s", "")
        qber    = result.get("qber")
        qber_s  = f"QBER={qber*100:.1f}%  " if qber is not None else ""
        err     = result.get("error_message", "")

        print(
            f"{status.upper():<8} "
            f"lat={latency}s  "
            f"sifted={result.get('n_sifted','?')}/{n}  "
            f"{qber_s}"
            + (f"[{err}]" if err else "")
        )

        if status == "done":
            passed += 1
        else:
            failed += 1

        # Cleanup on timeout — prevents zombie QKDL + 409 on next session
        if timed_out:
            cleanup_after_timeout(session_id, qkdl_url)

        row = session_to_row(case, result, timed_out)
        if latency:
            row["latency_s"] = latency
        append_row(csv_path, row, write_header)
        write_header = False

        # Inter-session pause (must be > QKDL cooldown 2.5s)
        time.sleep(INTER_SESSION_DELAY)

    elapsed_total = round(time.time() - t_suite_start, 1)
    print(f"\n{'='*65}")
    print(f"  DONE  total={total}  passed={passed}  failed={failed}")
    print(f"  Wall time: {elapsed_total}s ({elapsed_total/60:.1f} min)")
    print(f"  Results: {csv_path}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()