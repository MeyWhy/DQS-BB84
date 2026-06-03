
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Configuration — adapter si vos ports sont différents
# ---------------------------------------------------------------------------
ALICE_URL = os.getenv("ALICE_URL", "http://localhost:8001")
KME_URL   = os.getenv("KME_URL",   "http://localhost:8000")
QKDL_URL  = os.getenv("QKDL_URL",  "http://localhost:8003")

POLL_INTERVAL_S  = 1.5    # secondes entre chaque poll de statut
SESSION_TIMEOUT_S = 500   # timeout max par session
INTER_SESSION_S   = 3.0   # pause entre sessions (cooldown QKDL)

# ---------------------------------------------------------------------------
# Ansys Beer-Lambert formula — T(d) = 10^(-0.2*d/10)
# ---------------------------------------------------------------------------
def ansys_T(distance_km: float) -> float:
    if distance_km <= 0:
        return 1.0
    return 10 ** (-0.2 * distance_km / 10)

def ou_floor_pct(distance_km: float) -> float:
    """Estimation analytique du plancher QBER OU drift."""
    sigma = 0.01 + 0.001 * distance_km
    # E[sin²(θ/2)] pour OU de variance σ²/(2κ) avec κ=0.02
    variance = sigma**2 / (2 * 0.02)
    return math.sin(math.sqrt(variance) / 2)**2 * 100

def dark_floor_pct(distance_km: float) -> float:
    """Estimation analytique du plancher QBER dark counts."""
    dark = 1e-4
    signal = 0.85 * ansys_T(distance_km)
    if signal + dark == 0:
        return 0.0
    return 0.5 * dark / (signal + dark) * 100

def physical_floor_pct(distance_km: float) -> float:
    return ou_floor_pct(distance_km) + dark_floor_pct(distance_km)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def start_session(
    n_qubits: int,
    distance_km: float,
    interceptor_label: str = None,
    receiver_label: str = "bob-1",
) -> dict | None:
    params = {
        "receiver_label": receiver_label,
        "n_qubits":       n_qubits,
        "distance_km":    distance_km,
    }
    if interceptor_label:
        params["interceptor_label"] = interceptor_label
    try:
        r = httpx.post(f"{ALICE_URL}/start", params=params, timeout=30.0)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"    [ERR] start_session: {e}")
        return None


def poll_session(session_id: str, timeout_s: float = SESSION_TIMEOUT_S) -> dict | None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = httpx.get(f"{KME_URL}/sessions/{session_id}", timeout=10.0)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") in ("done", "aborted"):
                    return data
        except Exception:
            pass
        time.sleep(POLL_INTERVAL_S)
    return None


def get_channel_status(session_id: str) -> dict:
    try:
        r = httpx.get(f"{QKDL_URL}/channel/status/{session_id}", timeout=5.0)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------
def build_record(
    experiment_id: str,
    params: dict,
    start_response: dict,
    session_data: dict,
    channel_status: dict,
) -> dict:
    session_id  = session_data.get("session_id", "")
    status      = session_data.get("status", "unknown")
    success     = status == "done"
    n_qubits    = session_data.get("n_qubits",    params.get("n_qubits", 0))
    n_delivered = session_data.get("n_delivered", 0)
    n_sifted    = session_data.get("n_sifted",    0)
    qber        = session_data.get("qber",        0.0)
    elapsed_s   = session_data.get("elapsed_s",   0.0)
    key_final   = session_data.get("key_final",   "")
    key_len     = len(key_final)
    distance_km = session_data.get("distance_km", params.get("distance_km", 0.0))
    intercepted = session_data.get("intercepted", False)
    error_msg   = session_data.get("error_message", "")

    T_ansys        = ansys_T(distance_km)
    delivery_rate  = n_delivered / n_qubits if n_qubits > 0 else 0.0
    sift_rate      = n_sifted    / n_delivered if n_delivered > 0 else 0.0
    key_rate       = key_len     / n_qubits if n_qubits > 0 else 0.0
    # Rapport delivery_rate / T_ansys — idéalement ~0.85 (η du détecteur)
    delivery_vs_T  = delivery_rate / T_ansys if T_ansys > 0 else 0.0

    # Données optiques depuis /channel/status
    ch_data      = channel_status.get("channel", {})
    qber_budget  = channel_status.get("qber_budget", {})
    drift_state  = channel_status.get("drift_state", {})
    det_counters = channel_status.get("detector_counters", {})

    return {
        # Identification
        "experiment_id":       experiment_id,
        "session_id":          session_id,
        "timestamp":           datetime.now().isoformat(),

        # Paramètres de la session
        "distance_km":         distance_km,
        "n_qubits":            n_qubits,
        "intercepted":         intercepted,
        "interceptor_label":   session_data.get("interceptor_label", ""),

        # Résultat général
        "status":              status,
        "success":             success,
        "error_message":       error_msg,
        "elapsed_s":           round(elapsed_s, 3),

        # Métriques BB84
        "n_delivered":         n_delivered,
        "n_sifted":            n_sifted,
        "key_len":             key_len,
        "qber":                round(qber, 6),
        "qber_pct":            round(qber * 100, 4),

        # Taux dérivés
        "delivery_rate":       round(delivery_rate, 6),
        "sift_rate":           round(sift_rate, 6),
        "key_rate":            round(key_rate, 6),

        # Référence Ansys
        "ansys_T":             round(T_ansys, 6),
        "ansys_loss_db":       round(-10 * math.log10(max(T_ansys, 1e-12)), 3),
        "delivery_vs_ansys_T": round(delivery_vs_T, 4),

        # Planchers physiques théoriques
        "ou_floor_pct":        round(ou_floor_pct(distance_km), 6),
        "dark_floor_pct":      round(dark_floor_pct(distance_km), 6),
        "physical_floor_pct":  round(physical_floor_pct(distance_km), 6),

        # Optique live (depuis /channel/status, si disponible)
        "channel_type":        channel_status.get("channel_type", ""),
        "ansys_csv_loaded":    ch_data.get("ansys_csv_loaded", False),
        "ch_transmission":     round(ch_data.get("transmission_prob", 0.0), 6),
        "ch_qber_floor":       round(ch_data.get("qber_floor", 0.0), 8),
        "qber_budget_floor":   round(qber_budget.get("physical_floor", 0.0), 8),
        "qber_eve_threshold":  round(qber_budget.get("eve_threshold", 0.11), 6),

        # Détecteur live
        "det_photon_hits":     det_counters.get("photon_detections", 0),
        "det_dark_hits":       det_counters.get("dark_detections",   0),
        "det_missed":          det_counters.get("missed_photons",    0),
        "det_dead_blocked":    det_counters.get("dead_time_blocks",  0),

        # OU drift live
        "ou_current_theta_rad": round(drift_state.get("current_theta_rad", 0.0), 6),
        "ou_session_qber_est":  round(drift_state.get("session_qber_estimate", 0.0), 8),
        "ou_n_steps":           drift_state.get("n_steps", 0),
    }


# ---------------------------------------------------------------------------
# Experiment runner
# ---------------------------------------------------------------------------
def run_experiment(
    experiment_id: str,
    sessions: list[dict],
    results: list[dict],
    verbose: bool = True,
) -> None:
    total = len(sessions)
    for i, params in enumerate(sessions):
        d        = params["distance_km"]
        nq       = params["n_qubits"]
        eve      = params.get("interceptor_label", None)
        eve_str  = f" +Eve={eve}" if eve else ""
        prefix   = f"  [{i+1:>3}/{total}] d={d:>4}km n={nq:>5}{eve_str:15}"

        print(f"{prefix} → ", end="", flush=True)

        # Start session
        start_resp = start_session(
            n_qubits=nq,
            distance_km=d,
            interceptor_label=eve,
        )
        if not start_resp:
            print("FAILED (start)")
            results.append({
                "experiment_id": experiment_id,
                "distance_km":   d,
                "n_qubits":      nq,
                "status":        "error",
                "success":       False,
                "error_message": "start_session failed",
            })
            time.sleep(INTER_SESSION_S)
            continue

        session_id = start_resp.get("session_id", "")

        # Optionally grab channel status while session is running
        # (brief wait to let QKDL init)
        time.sleep(2.0)
        ch_status = get_channel_status(session_id)

        # Poll until done/aborted
        t0         = time.time()
        session_data = poll_session(session_id)
        elapsed    = time.time() - t0

        if not session_data:
            print(f"TIMEOUT after {elapsed:.0f}s")
            results.append({
                "experiment_id": experiment_id,
                "distance_km":   d,
                "n_qubits":      nq,
                "status":        "timeout",
                "success":       False,
                "error_message": "poll timeout",
                "elapsed_s":     elapsed,
            })
            time.sleep(INTER_SESSION_S)
            continue

        record = build_record(
            experiment_id, params, start_resp, session_data, ch_status
        )
        results.append(record)

        status_icon = "✓" if record["success"] else "✗"
        print(
            f"{status_icon} {record['status']:<8} "
            f"del={record['n_delivered']:>4} "
            f"sift={record['n_sifted']:>4} "
            f"key={record['key_len']:>4} "
            f"QBER={record['qber_pct']:>6.2f}% "
            f"T_ansys={record['ansys_T']:.4f} "
            f"del/T={record['delivery_vs_ansys_T']:.2f} "
            f"[{record['elapsed_s']:.1f}s]"
        )

        time.sleep(INTER_SESSION_S)


# ---------------------------------------------------------------------------
# Experiment definitions
# ---------------------------------------------------------------------------
def build_experiments(quick: bool = False) -> dict[str, list[dict]]:
    experiments = {}

    # --- Experiment 1: Distance sweep — effet Ansys T(d) ---
    distances_e1 = [0, 10, 20, 30, 40, 50, 60, 70, 80] if not quick else [0, 10, 30, 50, 80]
    n_e1         = 2000 if not quick else 1000
    experiments["E1_distance_sweep"] = [
        {"distance_km": d, "n_qubits": n_e1}
        for d in distances_e1
    ]

    # --- Experiment 2: n_qubits minimum par distance ---
    if not quick:
        dist_e2  = [10, 30, 50, 80]
        nq_e2    = [200, 500, 1000, 2000, 5000]
    else:
        dist_e2  = [10, 50]
        nq_e2    = [500, 2000]
    experiments["E2_nqubits_vs_distance"] = [
        {"distance_km": d, "n_qubits": n}
        for d in dist_e2
        for n in nq_e2
    ]

    # --- Experiment 3: Eve vs no-Eve ---
    dist_e3 = [10, 30, 50] if not quick else [10, 50]
    n_e3    = 2000 if not quick else 1000
    experiments["E3_eve_detection"] = []
    for d in dist_e3:
        experiments["E3_eve_detection"].append(
            {"distance_km": d, "n_qubits": n_e3}
        )
        experiments["E3_eve_detection"].append(
            {"distance_km": d, "n_qubits": n_e3, "interceptor_label": "eve-1"}
        )

    # --- Experiment 4: Variance statistique à 50 km ---
    reps = 5 if not quick else 3
    experiments["E4_variance_50km"] = [
        {"distance_km": 50, "n_qubits": 2000}
        for _ in range(reps)
    ]

    return experiments


# ---------------------------------------------------------------------------
# CSV / JSON export
# ---------------------------------------------------------------------------
FIELDNAMES = [
    "experiment_id", "session_id", "timestamp",
    "distance_km", "n_qubits", "intercepted", "interceptor_label",
    "status", "success", "error_message", "elapsed_s",
    "n_delivered", "n_sifted", "key_len", "qber", "qber_pct",
    "delivery_rate", "sift_rate", "key_rate",
    "ansys_T", "ansys_loss_db", "delivery_vs_ansys_T",
    "ou_floor_pct", "dark_floor_pct", "physical_floor_pct",
    "channel_type", "ansys_csv_loaded", "ch_transmission",
    "ch_qber_floor", "qber_budget_floor", "qber_eve_threshold",
    "det_photon_hits", "det_dark_hits", "det_missed", "det_dead_blocked",
    "ou_current_theta_rad", "ou_session_qber_est", "ou_n_steps",
]


def save_results(results: list[dict], out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path  = out_dir / f"bb84_optical_data_{ts}.csv"
    json_path = out_dir / f"bb84_optical_data_{ts}.json"

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow({k: r.get(k, "") for k in FIELDNAMES})

    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    return csv_path, json_path


# ---------------------------------------------------------------------------
# Plot generation
# ---------------------------------------------------------------------------
def generate_plots(csv_path: Path, out_dir: Path) -> None:
    try:
        import pandas as pd
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("\n  [INFO] matplotlib/pandas non installés — plots ignorés.")
        print("         pip install matplotlib pandas")
        return

    df = pd.read_csv(csv_path)
    df["success"] = df["success"].astype(str).str.lower() == "true"

    plt.rcParams.update({
        "font.size": 11, "axes.titlesize": 12,
        "axes.labelsize": 11, "figure.dpi": 150,
        "axes.spines.top": False, "axes.spines.right": False,
    })
    COLORS = {
        "ansys":   "#BA7517",
        "sim":     "#185FA5",
        "eve":     "#993C1D",
        "clean":   "#3B6D11",
        "floor":   "#533AB7",
        "failure": "#888780",
    }

    # ----------------------------------------------------------------
    # Figure 1 — Courbe Ansys T(d) vs taux de livraison mesuré
    # ----------------------------------------------------------------
    e1 = df[df["experiment_id"] == "E1_distance_sweep"].copy()
    if not e1.empty:
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
        fig.suptitle(
            "Expérience 1 — Effet de l'atténuation Ansys SMF-28 sur BB84\n"
            "T(d) = 10^(−0.2·d/10), α = 0.2 dB/km",
            fontsize=12,
        )

        dists     = sorted(e1["distance_km"].unique())
        ansys_T_v = [ansys_T(d) for d in dists]

        # Subplot A: T(d) théorique vs delivery rate mesuré
        ax = axes[0]
        e1_success = e1[e1["success"]]
        d_ok   = e1_success.groupby("distance_km")["delivery_rate"].mean()
        ax.plot(dists, ansys_T_v, "o-",
                color=COLORS["ansys"], lw=2, ms=6,
                label="T(d) Ansys (théorique)")
        if not d_ok.empty:
            ax.plot(d_ok.index, d_ok.values, "s--",
                    color=COLORS["sim"], lw=1.5, ms=6,
                    label="Taux livraison mesuré (sim.)")
            ax.fill_between(d_ok.index,
                            d_ok.values * 0.85, d_ok.values * 1.15,
                            alpha=0.15, color=COLORS["sim"],
                            label="±15% intervalle")
        ax.set_xlabel("Distance (km)")
        ax.set_ylabel("Taux de survie / livraison")
        ax.set_title("T(d) Ansys vs Delivery rate")
        ax.legend(fontsize=9)
        ax.set_ylim(0, 1.05)

        # Subplot B: bits de clé vs distance
        ax2 = axes[1]
        k_ok = e1_success.groupby("distance_km")["key_rate"].mean()
        k_th = [ansys_T(d) * 0.5 * 0.8 * 0.85 for d in dists]
        ax2.plot(dists, k_th, "o-",
                 color=COLORS["ansys"], lw=2, ms=6,
                 label="Taux clé théorique (T×0.5×0.8×η)")
        if not k_ok.empty:
            ax2.plot(k_ok.index, k_ok.values, "s--",
                     color=COLORS["sim"], lw=1.5, ms=6,
                     label="Taux clé mesuré")
        ax2.set_xlabel("Distance (km)")
        ax2.set_ylabel("Key rate (bits/qubit envoyé)")
        ax2.set_title("Taux de clé vs distance")
        ax2.legend(fontsize=9)

        plt.tight_layout()
        p = out_dir / "fig1_ansys_attenuation.png"
        plt.savefig(p, bbox_inches="tight")
        plt.close()
        print(f"  → {p}")

    # ----------------------------------------------------------------
    # Figure 2 — n_qubits minimum par distance
    # ----------------------------------------------------------------
    e2 = df[df["experiment_id"] == "E2_nqubits_vs_distance"].copy()
    if not e2.empty:
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
        fig.suptitle(
            "Expérience 2 — n_qubits minimum requis par distance\n"
            "(pour éviter INSUFFICIENT_BITS)",
            fontsize=12,
        )

        dist_vals = sorted(e2["distance_km"].unique())
        nq_vals   = sorted(e2["n_qubits"].unique())
        cmap      = plt.cm.Blues(np.linspace(0.4, 0.9, len(nq_vals)))

        ax = axes[0]
        for nq, c in zip(nq_vals, cmap):
            sub = e2[e2["n_qubits"] == nq].groupby("distance_km").agg(
                success_rate=("success", "mean"),
                mean_key=("key_len", "mean"),
            )
            ax.plot(sub.index, sub["success_rate"], "o-",
                    color=c, lw=1.5, ms=5, label=f"n={nq}")
        ax.axhline(0.8, color="#aaa", ls="--", lw=1, label="80% succès")
        ax.set_xlabel("Distance (km)")
        ax.set_ylabel("Taux de succès")
        ax.set_title("Taux de succès vs (distance, n_qubits)")
        ax.legend(fontsize=9, ncol=2)
        ax.set_ylim(-0.05, 1.1)

        ax2 = axes[1]
        for nq, c in zip(nq_vals, cmap):
            sub = e2[e2["n_qubits"] == nq].groupby("distance_km")["key_len"].mean()
            if not sub.empty:
                ax2.plot(sub.index, sub.values, "o-",
                         color=c, lw=1.5, ms=5, label=f"n={nq}")
        ax2.set_xlabel("Distance (km)")
        ax2.set_ylabel("Bits de clé (moyenne)")
        ax2.set_title("Bits de clé vs (distance, n_qubits)")
        ax2.legend(fontsize=9, ncol=2)

        plt.tight_layout()
        p = out_dir / "fig2_nqubits_distance.png"
        plt.savefig(p, bbox_inches="tight")
        plt.close()
        print(f"  → {p}")

    # ----------------------------------------------------------------
    # Figure 3 — Eve vs no-Eve: QBER par distance
    # ----------------------------------------------------------------
    e3 = df[df["experiment_id"] == "E3_eve_detection"].copy()
    if not e3.empty:
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
        fig.suptitle(
            "Expérience 3 — Détection d'Eve par QBER\n"
            "Intercept-resend attack · seuil de détection = 11%",
            fontsize=12,
        )

        e3_clean = e3[~e3["intercepted"]]
        e3_eve   = e3[e3["intercepted"]]
        dist_e3  = sorted(e3["distance_km"].unique())

        ax = axes[0]
        if not e3_clean.empty:
            qc = e3_clean.groupby("distance_km")["qber_pct"].mean()
            ax.plot(qc.index, qc.values, "o-",
                    color=COLORS["clean"], lw=2, ms=6, label="Sans Eve")
        if not e3_eve.empty:
            qe = e3_eve.groupby("distance_km")["qber_pct"].mean()
            ax.plot(qe.index, qe.values, "s-",
                    color=COLORS["eve"], lw=2, ms=6, label="Avec Eve (100%)")
        ax.axhline(11.0, color="#993C1D", ls="--", lw=1.5, label="Seuil 11%")
        pf = [physical_floor_pct(d) for d in dist_e3]
        ax.plot(dist_e3, pf, ":",
                color=COLORS["floor"], lw=1.5, label="Plancher physique")
        ax.set_xlabel("Distance (km)")
        ax.set_ylabel("QBER (%)")
        ax.set_title("QBER avec/sans Eve")
        ax.legend(fontsize=9)

        ax2 = axes[1]
        labels  = ["Sans Eve", "Avec Eve"]
        for label_str, sub_df, color in [
            ("Sans Eve", e3_clean, COLORS["clean"]),
            ("Avec Eve", e3_eve, COLORS["eve"]),
        ]:
            if not sub_df.empty:
                aborts = sub_df.groupby("distance_km").apply(
                    lambda x: (x["status"] == "aborted").mean() * 100
                )
                ax2.plot(aborts.index, aborts.values, "o-",
                         color=color, lw=2, ms=6, label=label_str)
        ax2.set_xlabel("Distance (km)")
        ax2.set_ylabel("Taux d'abort (%)")
        ax2.set_title("Taux d'annulation de session")
        ax2.legend(fontsize=9)
        ax2.set_ylim(-5, 105)

        plt.tight_layout()
        p = out_dir / "fig3_eve_detection.png"
        plt.savefig(p, bbox_inches="tight")
        plt.close()
        print(f"  → {p}")

    # ----------------------------------------------------------------
    # Figure 4 — Variance statistique à 50 km
    # ----------------------------------------------------------------
    e4 = df[df["experiment_id"] == "E4_variance_50km"].copy()
    if not e4.empty:
        fig, axes = plt.subplots(1, 3, figsize=(14, 4))
        fig.suptitle(
            "Expérience 4 — Variance statistique à 50 km, n=2000\n"
            "T(50km)=0.10 · chaque session est un tirage indépendant",
            fontsize=12,
        )
        e4_ok = e4[e4["success"]]

        for ax, col, title, color in [
            (axes[0], "n_delivered", "Photons livrés", COLORS["ansys"]),
            (axes[1], "n_sifted",    "Bits siftés",    COLORS["sim"]),
            (axes[2], "key_len",     "Bits de clé",    COLORS["clean"]),
        ]:
            vals = e4_ok[col].values if not e4_ok.empty else []
            if len(vals) > 0:
                ax.hist(vals, bins=max(3, len(vals)//2),
                        color=color, alpha=0.75, edgecolor="white")
                ax.axvline(np.mean(vals), color="#333",
                           ls="--", lw=1.5, label=f"μ={np.mean(vals):.1f}")
                ax.legend(fontsize=9)
            ax.set_xlabel(col)
            ax.set_ylabel("Fréquence")
            ax.set_title(title)

        plt.tight_layout()
        p = out_dir / "fig4_variance_50km.png"
        plt.savefig(p, bbox_inches="tight")
        plt.close()
        print(f"  → {p}")

    # ----------------------------------------------------------------
    # Figure 5 — Tableau récapitulatif Ansys
    # ----------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(12, 3.5))
    ax.axis("off")
    fig.suptitle(
        "Tableau récapitulatif — Canal optique Ansys SMF-28 à 1550 nm",
        fontsize=12, y=1.01,
    )
    distances_ref = [0, 10, 20, 30, 50, 80, 100]
    table_data = []
    for d in distances_ref:
        T = ansys_T(d)
        table_data.append([
            f"{d} km",
            f"{T:.5f}",
            f"{-10*math.log10(max(T,1e-12)):.2f} dB",
            f"{T*100:.3f}%",
            f"{physical_floor_pct(d):.5f}%",
            f"{max(0, 11 - physical_floor_pct(d)):.4f}%",
            f"{int(T * 0.85 * 0.5 * 0.8 * 10000):>5} / 10k",
        ])
    columns = [
        "Distance", "T(d)", "Perte (dB)", "Survie %",
        "Plancher QBER", "Marge Eve", "Bits clé / 10k",
    ]
    tbl = ax.table(
        cellText=table_data,
        colLabels=columns,
        cellLoc="center",
        loc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.6)
    for (r, c), cell in tbl.get_celld().items():
        if r == 0:
            cell.set_facecolor("#BA7517")
            cell.set_text_props(color="white", fontweight="bold")
        elif r % 2 == 0:
            cell.set_facecolor("#FEF8EE")
    plt.tight_layout()
    p = out_dir / "fig5_ansys_reference_table.png"
    plt.savefig(p, bbox_inches="tight")
    plt.close()
    print(f"  → {p}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Collecte de données BB84 optique Ansys"
    )
    parser.add_argument("--quick",  action="store_true",
                        help="Mode rapide (moins de sessions)")
    parser.add_argument("--out",    default="./resultats_bb84",
                        help="Dossier de sortie")
    parser.add_argument("--no-plots", action="store_true",
                        help="Ne pas générer les graphiques")
    parser.add_argument("--exp",    nargs="+",
                        help="Lancer seulement ces expériences (ex: E1 E3)")
    args = parser.parse_args()

    out_dir = Path(args.out)
    print(f"\n{'='*60}")
    print(f"  BB84 Optical Data Collection — Ansys SMF-28")
    print(f"{'='*60}")
    print(f"  Alice : {ALICE_URL}")
    print(f"  KME   : {KME_URL}")
    print(f"  QKDL  : {QKDL_URL}")
    print(f"  Output: {out_dir}")
    print(f"  Mode  : {'rapide' if args.quick else 'complet'}")
    print(f"{'='*60}\n")

    # Vérifier que Alice répond
    try:
        r = httpx.get(f"{ALICE_URL}/health", timeout=5.0)
        print(f"  Alice health: {r.json().get('status', 'ok')}\n")
    except Exception as e:
        print(f"  [WARN] Alice unreachable: {e}")
        print(f"  Assurez-vous que le système BB84 est démarré.\n")

    experiments = build_experiments(quick=args.quick)
    if args.exp:
        experiments = {
            k: v for k, v in experiments.items()
            if any(k.startswith(e) for e in args.exp)
        }

    all_results: list[dict] = []

    for exp_id, sessions in experiments.items():
        n_total = len(sessions)
        print(f"\n{'─'*60}")
        print(f"  {exp_id}  ({n_total} sessions)")
        print(f"{'─'*60}")
        run_experiment(exp_id, sessions, all_results)

    print(f"\n{'='*60}")
    print(f"  Total sessions: {len(all_results)}")
    ok = sum(1 for r in all_results if r.get("success"))
    print(f"  Succès: {ok} / {len(all_results)}")

    csv_path, json_path = save_results(all_results, out_dir)
    print(f"\n  CSV  → {csv_path}")
    print(f"  JSON → {json_path}")

    if not args.no_plots:
        print(f"\n  Génération des graphiques...")
        generate_plots(csv_path, out_dir)

    print(f"\n  Terminé. Dossier: {out_dir}\n")


if __name__ == "__main__":
    main()