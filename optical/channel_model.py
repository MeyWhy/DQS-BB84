from __future__ import annotations

import csv
import math
import os
from pathlib import Path
from typing import Callable

#scipy is optional - pure Python fallback is used if not installed
try:
    from scipy.interpolate import interp1d as _scipy_interp1d
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False

_DATA_DIR    = Path(__file__).parent / "data"
_DEFAULT_CSV = _DATA_DIR / "attenuation_table.csv"


def _linear_interp(xs: list[float], ys: list[float]) -> Callable[[float], float]:
 
    def _interp(x: float) -> float:
        if x <= xs[0]:
            return ys[0]
        if x >= xs[-1]:
            return ys[-1]
        # Binary search for the interval
        lo, hi = 0, len(xs) - 1
        while lo + 1 < hi:
            mid = (lo + hi) // 2
            if xs[mid] <= x:
                lo = mid
            else:
                hi = mid
        t = (x - xs[lo]) / (xs[hi] - xs[lo])
        return ys[lo] + t * (ys[hi] - ys[lo])
    return _interp


class ChannelModel:

    def __init__(
        self,
        csv_path:       str | Path = _DEFAULT_CSV,
        pmd_csv_path:   str | Path | None = None,
        alpha_fallback: float = 0.2,
    ):
        self._alpha_fallback = alpha_fallback
        self._pmd_fn: Callable[[float], float] | None = None

        # --- Load attenuation table ---
        csv_path = Path(csv_path)
        distances, transmissions = self._load_csv(csv_path)

        if distances:
            self._T_fn          = self._build_interp(distances, transmissions)
            self.max_distance_km = max(distances)
            self.source          = "csv"
            self._csv_path       = csv_path
        else:
            # Fallback: analytical formula (no CSV needed)
            self._T_fn           = lambda d: 10 ** (-(alpha_fallback * d) / 10)
            self.max_distance_km = 120.0
            self.source          = "fallback"
            self._csv_path       = None

        # Load PMD table
        if pmd_csv_path is not None:
            pmd_path = Path(pmd_csv_path)
            pmd_d, pmd_phi = self._load_pmd_csv(pmd_path)
            if pmd_d:
                phi_fn        = self._build_interp(pmd_d, pmd_phi)
                # P(flip) = sin²(Δφ/2) - standard Malus-law QBER from phase shift
                self._pmd_fn  = lambda d: math.sin(phi_fn(d) / 2) ** 2
            else:
                self._pmd_fn  = None

    def transmission_prob(self, distance_km: float) -> float:
        """
        Fraction of photons expected to survive at this distance.

        Clamps to [0, 1]. Returns 1.0 at distance=0.
        For distances beyond the CSV range, returns the last CSV value
        (conservative - typically near 0 for long distances).
        """
        if distance_km <= 0:
            return 1.0
        return max(0.0, min(1.0, self._T_fn(distance_km)))

    def qber_floor(self, distance_km: float = 0.0) -> float:
        """
        Physical QBER floor from polarization drift at this distance.

        Step 1: returns 0.0 (no PMD model loaded yet).
        Step 3: returns sin²(Δφ/2) from the PMD CSV.
        """
        if self._pmd_fn is None:
            return 0.0
        return max(0.0, min(0.5, self._pmd_fn(distance_km)))

    def describe(self, distance_km: float = 0.0) -> dict:
        return {
            "model":            "ansys_csv_channel",
            "source":           self.source,
            "csv_path":         str(self._csv_path) if self._csv_path else None,
            "distance_km":      distance_km,
            "transmission_prob": self.transmission_prob(distance_km),
            "loss_db":          (
                round(-10 * math.log10(max(self.transmission_prob(distance_km), 1e-12)), 3)
            ),
            "qber_floor":        self.qber_floor(distance_km),
            "max_distance_km":  self.max_distance_km,
            "pmd_loaded":       self._pmd_fn is not None,
            "scipy_used":       _SCIPY_AVAILABLE,
        }
    @staticmethod
    def _load_csv(path: Path) -> tuple[list[float], list[float]]:
        if not path.exists():
            return [], []
        distances, transmissions = [], []
        try:
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    d = float(row["distance_km"])
                    t = float(row["transmission_prob"])
                    if 0.0 < t <= 1.0 and d >= 0:
                        distances.append(d)
                        transmissions.append(t)
        except Exception:
            return [], []
        return distances, transmissions

    @staticmethod
    def _load_pmd_csv(path: Path) -> tuple[list[float], list[float]]:
        """Load distance_km and delta_phi_rad columns from PMD CSV."""
        if not path.exists():
            return [], []
        distances, phis = [], []
        try:
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    d   = float(row["distance_km"])
                    phi = float(row["delta_phi_rad"])
                    distances.append(d)
                    phis.append(phi)
        except Exception:
            return [], []
        return distances, phis

    @staticmethod
    def _build_interp(xs: list[float], ys: list[float]) -> Callable[[float], float]:
        """Build interpolation function - scipy if available, pure Python otherwise."""
        if _SCIPY_AVAILABLE:
            fn = _scipy_interp1d(
                xs, ys,
                kind="linear",
                bounds_error=False,
                fill_value=(ys[0], ys[-1]),  # clamp at boundaries
            )
            return lambda d: float(fn(d))
        return _linear_interp(xs, ys)
    
    """
This module is the single bridge between Ansys and your BB84 simulator.

    Ansys Lumerical
        ↓  (exports)
    optical/data/attenuation_table.csv
        ↓  (loaded once at startup by)
    ChannelModel
        ↓  (queried per photon by)
    FiberChannel.transmit()
        ↓
    BB84 pipeline (qunetsim_service.py)

ChannelModel does exactly three things:
  1. Load the CSV (once, at __init__)
  2. Interpolate T(d) for any distance in [0, max_distance_km]
  3. Expose transmission_prob(d) and qber_floor(d) - same interface
     as StatisticalChannel so the rest of the code never changes


"""