from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt

DEFAULT_CSV = Path(__file__).parent / "tests" / "results" / "multi_distance_results.csv"
OUT_DIR = Path(__file__).parent / "results"

QBER_ABORT_THRESHOLD = 0.11  # mirrors bb84_logic.QBER_THRESHOLD


def load_rows(csv_path: Path) -> list[dict]:
    rows = []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            row["distance_km"] = float(row["distance_km"])
            row["n_qubits"] = int(row["n_qubits"]) if row["n_qubits"] else 0
            row["n_sifted"] = int(row["n_sifted"]) if row["n_sifted"] else 0
            row["n_delivered"] = int(row["n_delivered"]) if row["n_delivered"] else 0
            row["qber"] = float(row["qber"]) if row["qber"] not in ("", None) else None
            rows.append(row)
    return rows


def group_by_distance(rows: list[dict]) -> dict[float, list[dict]]:
    groups: dict[float, list[dict]] = defaultdict(list)
    for r in rows:
        groups[r["distance_km"]].append(r)
    return dict(sorted(groups.items()))


def plot_qber(groups: dict[float, list[dict]], out_path: Path) -> None:
    distances, qber_means, qber_mins, qber_maxs, n_points = [], [], [], [], []
    for d, rows in groups.items():
        qbers = [r["qber"] for r in rows if r["qber"] is not None]
        if not qbers:
            continue
        distances.append(d)
        qber_means.append(sum(qbers) / len(qbers))
        qber_mins.append(min(qbers))
        qber_maxs.append(max(qbers))
        n_points.append(len(qbers))

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(distances, qber_means, "o-", color="#1b4965", label="Mean QBER (successful sessions)")
    ax.fill_between(distances, qber_mins, qber_maxs, color="#1b4965", alpha=0.15, label="min-max range")
    ax.axhline(QBER_ABORT_THRESHOLD, color="#e07a5f", linestyle="--", label=f"Abort threshold ({QBER_ABORT_THRESHOLD*100:.0f}%)")
    for d, n in zip(distances, n_points):
        ax.annotate(f"n={n}", (d, 0), textcoords="offset points", xytext=(0, -18), fontsize=7, ha="center", color="#666")
    ax.set_xlabel("Distance (km)")
    ax.set_ylabel("QBER")
    ax.set_title("Measured QBER vs. Distance (successful sessions only)")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Saved: {out_path}")


def plot_viability(groups: dict[float, list[dict]], out_path: Path) -> None:
    distances, success_rate, abort_rate, other_rate = [], [], [], []
    for d, rows in groups.items():
        n = len(rows)
        n_done = sum(1 for r in rows if r["status"] == "done")
        n_aborted = sum(1 for r in rows if r["status"] == "aborted")
        n_other = n - n_done - n_aborted
        distances.append(d)
        success_rate.append(n_done / n)
        abort_rate.append(n_aborted / n)
        other_rate.append(n_other / n)

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.bar(distances, success_rate, width=3.0, color="#2a9d8f", label="done")
    ax.bar(distances, abort_rate, width=3.0, bottom=success_rate, color="#e07a5f", label="aborted")
    bottoms = [s + a for s, a in zip(success_rate, abort_rate)]
    ax.bar(distances, other_rate, width=3.0, bottom=bottoms, color="#888888", label="other (timeout/create_failed)")
    ax.set_xlabel("Distance (km)")
    ax.set_ylabel("Fraction of runs")
    ax.set_title("Session Outcome vs. Distance")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Saved: {out_path}")


def plot_sample_size(groups: dict[float, list[dict]], out_path: Path) -> None:
    distances, sifted_means, qubits = [], [], []
    for d, rows in groups.items():
        distances.append(d)
        sifted_means.append(sum(r["n_sifted"] for r in rows) / len(rows))
        qubits.append(rows[0]["n_qubits"])  # same n_qubits across repeats at a given distance

    fig, ax1 = plt.subplots(figsize=(7, 4.5))
    ax1.plot(distances, sifted_means, "o-", color="#1b4965", label="Mean n_sifted")
    ax1.axhline(10, color="#e07a5f", linestyle="--", label="INSUFFICIENT_BITS floor (n=10)")
    ax1.set_xlabel("Distance (km)")
    ax1.set_ylabel("n_sifted")
    ax1.legend(fontsize=8, loc="upper right")
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(distances, qubits, "s--", color="#888888", alpha=0.6, label="n_qubits sent")
    ax2.set_ylabel("n_qubits sent (per session)")
    ax2.legend(fontsize=8, loc="lower right")

    ax1.set_title("Sample Size vs. Distance")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"Saved: {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"No results file at {args.csv} — run test_multi_distance.py first.")
        return

    rows = load_rows(args.csv)
    groups = group_by_distance(rows)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plot_qber(groups, OUT_DIR / "multi_distance_qber.png")
    plot_viability(groups, OUT_DIR / "multi_distance_viability.png")
    plot_sample_size(groups, OUT_DIR / "multi_distance_sample_size.png")


if __name__ == "__main__":
    main()