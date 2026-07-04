"""
debug_fiber_loss.py — Diagnostic for anomalous n_delivered at 10km.

Reproduces the exact FiberChannel.transmit() pipeline used in
_process_batch_sync (qunetsim_service.py) at 10km, with detailed counters
at each sub-step to isolate where photons are lost.

Run from the project root (where optical/ lives):
    python debug_fiber_loss.py

No running services needed.
"""
from __future__ import annotations

import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from optical.channel import FiberChannel, StatisticalChannel
from optical.detector import SinglePhotonDetector
from optical.polarization import OUDriftChannel

PULSE_PERIOD_NS = 1000.0   # mirrors qunetsim_service.py


def _simulate_fiber(
    distance_km: float,
    n_qubits: int,
    n_runs: int = 10,
    seed: int = 42,
) -> None:
    """
    Run n_runs independent batches of n_qubits through FiberChannel at
    distance_km, counting exactly where each photon is lost.
    """
    random.seed(seed)

    # Reproduce the exact FiberChannel construction from NetworkSession in
    # qunetsim_service.py (no csv_path, no pmd_csv_path, all defaults)
    from optical.ansys_export import generate_synthetic_csv, generate_synthetic_pmd_csv
    data_dir = Path(__file__).parent / "optical" / "data"
    csv_path = str(generate_synthetic_csv(data_dir / "attenuation_table.csv"))
    pmd_csv_path = str(generate_synthetic_pmd_csv(data_dir / "pmd_table.csv"))

    print(f"\n{'='*60}")
    print(f"FiberChannel diagnostic: distance={distance_km}km, n_qubits={n_qubits}")
    print(f"Runs: {n_runs}, Seed: {seed}")
    print(f"{'='*60}")

    # Theoretical expectations
    alpha = 0.2
    T_theory = 10 ** (-(alpha * distance_km) / 10) if distance_km > 0 else 1.0
    eta = 0.85
    dark_hz = 100.0
    clock_hz = 1_000_000.0
    dark_prob = dark_hz / clock_hz
    E_signal = n_qubits * T_theory * eta
    E_dark   = n_qubits * dark_prob   # rough: dark fires independently
    print(f"\nTheory:")
    print(f"  T(d)              = {T_theory:.6f}  ({T_theory*100:.2f}%)")
    print(f"  E[signal detects] = {E_signal:.1f}  ({E_signal/n_qubits*100:.1f}% of sent)")
    print(f"  dark_prob/pulse   = {dark_prob:.2e}")
    print(f"  E[dark detects]   = {E_dark:.3f}")

    totals = {
        "sent":             0,
        "lost_attenuation": 0,
        "lost_detector_eta":0,
        "lost_dead_time":   0,
        "dark_count":       0,
        "signal_detected":  0,
    }

    for run in range(n_runs):
        # Fresh channel per run — mirrors reset_session() in NetworkSession
        fc = FiberChannel(
            distance_km=distance_km,
            csv_path=csv_path,
            pmd_csv_path=pmd_csv_path,
            alpha_db_per_km=alpha,
            enable_drift=True,
            enable_detector=True,
            use_ou_drift=True,
            eta=eta,
            dark_count_hz=dark_hz,
            dead_time_ns=50.0,
        )
        fc.reset_session()

        run_counts = {k: 0 for k in totals}

        for i in range(n_qubits):
            t_ns = i * PULSE_PERIOD_NS
            basis = random.choice(["Z", "X"])
            bit   = random.randint(0, 1)
            photon = {"qubit_id": i, "bit": bit, "basis": basis}

            run_counts["sent"] += 1

            # --- Step 1: attenuation (Beer-Lambert via CSV or analytical) ---
            # We have to reproduce the logic manually to count sub-steps.
            # transmit() bundles attenuation + drift + detector together,
            # so we use the channel's sub-objects directly here.

            # Attenuation
            T = fc._transmission
            survived_attenuation = random.random() <= T

            if not survived_attenuation:
                run_counts["lost_attenuation"] += 1
                # Also run the detector with None so dead-time state advances
                # consistently with what transmit() does internally.
                if fc.detector:
                    fc.detector.detect(None, t_ns)
                continue

            # Drift (OU) — may flip basis/bit but doesn't lose the photon
            if fc.drift:
                from optical.channel import _ENCODE, _DECODE
                phys = _ENCODE.get((basis, bit))
                if phys:
                    drifted = fc.drift.apply(phys)
                    if drifted != phys:
                        new_basis, new_bit = _DECODE[drifted]
                        photon = {**photon, "basis": new_basis, "bit": new_bit}

            # --- Step 2: detector ---
            if fc.detector:
                clicked, reason = fc.detector.detect(photon, t_ns)
                if not clicked:
                    reason_map = {
                        "missed": "lost_detector_eta",
                        "dead":   "lost_dead_time",
                    }
                    run_counts[reason_map.get(reason, "lost_detector_eta")] += 1
                    continue
                if reason == "dark":
                    run_counts["dark_count"] += 1
                else:
                    run_counts["signal_detected"] += 1
            else:
                run_counts["signal_detected"] += 1

        for k in totals:
            totals[k] += run_counts[k]

    # Average over runs
    print(f"\nPer-run averages over {n_runs} runs ({n_qubits} qubits each):")
    print(f"  {'Step':<30}  {'Count':>8}  {'% of sent':>10}")
    print(f"  {'-'*52}")
    sent_avg = totals["sent"] / n_runs
    for label, key in [
        ("Sent",                     "sent"),
        ("→ Lost: fiber attenuation","lost_attenuation"),
        ("→ Lost: detector missed",  "lost_detector_eta"),
        ("→ Lost: detector dead time","lost_dead_time"),
        ("→ Dark count (delivered)", "dark_count"),
        ("→ Signal detected (deliv.)","signal_detected"),
    ]:
        avg = totals[key] / n_runs
        pct = avg / sent_avg * 100 if sent_avg > 0 else 0
        print(f"  {label:<30}  {avg:>8.1f}  {pct:>9.1f}%")

    delivered_avg = (totals["dark_count"] + totals["signal_detected"]) / n_runs
    print(f"\n  {'Total delivered (sig+dark)':<30}  {delivered_avg:>8.1f}  {delivered_avg/sent_avg*100:>9.1f}%")
    print(f"  Theory E[delivered]           ~{E_signal + E_dark:>8.1f}  {(E_signal+E_dark)/n_qubits*100:>9.1f}%")

    # Dead time check: at 1MHz clock, dead_time=50ns -> only 5% of qubit
    # slots fall within one dead time window after a click. Show this clearly.
    max_clock = 1e9 / 50.0  # = 20 MHz max before dead time dominates
    actual_clock = clock_hz
    dead_fraction_theoretical = min(1.0, delivered_avg / (1e9 / 50.0 / actual_clock * n_qubits))
    print(f"\nDead time check:")
    print(f"  Dead time window: 50 ns")
    print(f"  Qubit clock period: {PULSE_PERIOD_NS:.0f} ns")
    print(f"  At 1 MHz clock: dead time blocks only the very next slot after a click")
    print(f"  Expected dead-time blocking rate: ~{delivered_avg/n_qubits*100:.1f}% of {n_qubits} → ~{delivered_avg*50/1000:.2f} blocked")
    print(f"  Actual dead-time blocked (observed): {totals['lost_dead_time']/n_runs:.1f} per run")


def compare_distances(n_qubits: int = 200, n_runs: int = 5) -> None:
    """
    Quick comparison table across distances to show where the model breaks
    from theoretical expectations.
    """
    from optical.ansys_export import generate_synthetic_csv, generate_synthetic_pmd_csv
    data_dir = Path(__file__).parent / "optical" / "data"
    csv_path = str(generate_synthetic_csv(data_dir / "attenuation_table.csv"))
    pmd_csv_path = str(generate_synthetic_pmd_csv(data_dir / "pmd_table.csv"))

    print(f"\n{'='*70}")
    print(f"Delivery rate comparison across distances (n_qubits={n_qubits}, {n_runs} runs each)")
    print(f"{'='*70}")
    print(f"{'d (km)':>8}  {'T_theory':>10}  {'E[deliv]':>10}  {'Obs deliv':>10}  {'Ratio obs/exp':>14}")
    print(f"{'-'*58}")

    alpha = 0.2
    eta   = 0.85
    dark_prob = 100.0 / 1_000_000.0

    for d in [0, 5, 10, 25, 50, 100]:
        T = 10 ** (-(alpha * d) / 10) if d > 0 else 1.0
        expected = n_qubits * (T * eta + dark_prob)

        total_delivered = 0
        for run in range(n_runs):
            random.seed(run)
            fc = FiberChannel(
                distance_km=d,
                csv_path=csv_path,
                pmd_csv_path=pmd_csv_path,
                enable_drift=True,
                enable_detector=True,
                use_ou_drift=True,
            )
            fc.reset_session()
            for i in range(n_qubits):
                t_ns  = i * PULSE_PERIOD_NS
                basis = random.choice(["Z", "X"])
                bit   = random.randint(0, 1)
                out   = fc.transmit({"qubit_id": i, "bit": bit, "basis": basis}, t_ns=t_ns)
                if out is not None:
                    total_delivered += 1

        obs = total_delivered / n_runs
        ratio = obs / expected if expected > 0 else float("nan")
        flag = "  ← ?" if ratio < 0.3 else ""
        print(f"{d:>8.0f}  {T:>10.6f}  {expected:>10.1f}  {obs:>10.1f}  {ratio:>14.3f}{flag}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--distance", type=float, default=10.0,
                        help="Distance to debug in detail (default: 10km)")
    parser.add_argument("--n-qubits", type=int, default=224,
                        help="n_qubits to simulate (default: 224, matches 10km plan)")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--compare", action="store_true",
                        help="Run a quick comparison table across all distances instead")
    args = parser.parse_args()

    if args.compare:
        compare_distances(n_qubits=args.n_qubits, n_runs=args.runs)
    else:
        _simulate_fiber(args.distance, args.n_qubits, args.runs)
        # Also run 0km for comparison baseline
        if args.distance != 0:
            _simulate_fiber(0.0, args.n_qubits, args.runs)
