from __future__ import annotations
import logging
import random

logger = logging.getLogger("optical.detector")


class SinglePhotonDetector:
    """
    Step 3 — Single photon detector with realistic imperfections.

    Three physically distinct noise sources, each independently modelled:

    η (efficiency)
        Even when a photon arrives, the detector misses it with probability
        1 − η. For SNSPDs (superconducting nanowire): η ≈ 0.85–0.95.
        For silicon APDs (cheaper, room-temp): η ≈ 0.50–0.70.

    Dark counts
        The detector fires spontaneously with no photon present.
        Caused by thermal noise and quantum tunnelling. Specified as a
        rate in Hz; converted to per-pulse probability using the system
        clock rate. Commercial SNSPD: ~100 Hz. APD: ~1000–10000 Hz.

    Dead time
        After any click (real or dark), the detector is blind for
        dead_time_ns nanoseconds. Limits maximum sustainable count rate.
        SNSPD: ~50 ns. APD: ~10–50 μs (much longer).

    Parameters
    ----------
    eta : float
        Detection efficiency in [0, 1].
    dark_count_hz : float
        Dark count rate in Hz.
    dead_time_ns : float
        Recovery time after a detection in nanoseconds.
    clock_rate_hz : float
        System pulse rate in Hz. Used to convert dark_count_hz to a
        per-pulse probability. Default 1 MHz matches typical QKD systems.
    """

    def __init__(
        self,
        eta:           float = 0.85,
        dark_count_hz: float = 100.0,
        dead_time_ns:  float = 50.0,
        clock_rate_hz: float = 1_000_000.0,
    ):
        if not 0.0 < eta <= 1.0:
            raise ValueError(f"eta must be in (0, 1], got {eta}")
        if dark_count_hz < 0:
            raise ValueError(f"dark_count_hz must be >= 0, got {dark_count_hz}")
        if dead_time_ns < 0:
            raise ValueError(f"dead_time_ns must be >= 0, got {dead_time_ns}")

        self.eta           = eta
        self.dark_count_hz = dark_count_hz
        self.dead_time_ns  = dead_time_ns
        self.clock_rate_hz = clock_rate_hz

        # Probability of a dark count per pulse window
        self._dark_prob_per_pulse = dark_count_hz / clock_rate_hz

        # Track last click time for dead time enforcement
        self._last_click_ns: float = -dead_time_ns - 1.0

        # Diagnostic counters (reset per session via reset_counters())
        self._n_photon_detections = 0
        self._n_dark_detections   = 0
        self._n_missed_photons    = 0
        self._n_dead_time_blocks  = 0

    # ------------------------------------------------------------------
    # Core detection logic
    # ------------------------------------------------------------------

    def detect(self, photon: dict | None, t_ns: float) -> tuple[bool, str]:
        """
        Attempt to detect a photon (or generate a dark count) at time t_ns.

        Parameters
        ----------
        photon : dict | None
            Photon record from channel.transmit(), or None if photon was lost.
        t_ns : float
            Current simulation time in nanoseconds. Used for dead time.

        Returns
        -------
        (clicked, reason) : tuple[bool, str]
            clicked — True if the detector fired.
            reason  — one of: "signal", "dark", "dead", "missed", "no_photon"
                      Used for diagnostics; not needed by the caller in normal use.
        """
        # Dead time check — detector is blind regardless of photon or dark count
        if (t_ns - self._last_click_ns) < self.dead_time_ns:
            self._n_dead_time_blocks += 1
            return False, "dead"

        # Dark count fires independently of photon presence
        if random.random() < self._dark_prob_per_pulse:
            self._last_click_ns = t_ns
            self._n_dark_detections += 1
            logger.debug("Dark count at t=%.1f ns", t_ns)
            return True, "dark"

        # Real photon detection
        if photon is not None:
            if random.random() < self.eta:
                self._last_click_ns = t_ns
                self._n_photon_detections += 1
                return True, "signal"
            else:
                self._n_missed_photons += 1
                return False, "missed"

        return False, "no_photon"

    # ------------------------------------------------------------------
    # Derived metrics
    # ------------------------------------------------------------------

    def qber_contribution(self, fiber_transmission: float) -> float:
        """
        Analytical QBER floor from dark counts at a given fiber transmission.

        Formula: QBER_dark = 0.5 × d / (η·T + d)

        where d = dark_prob_per_pulse, T = fiber transmission.

        Dark counts are random bits → 50% of them are wrong → factor 0.5.
        This is the dominant QBER source at long distances (small T).
        """
        d = self._dark_prob_per_pulse
        signal = self.eta * fiber_transmission
        if signal + d == 0:
            return 0.0
        return 0.5 * d / (signal + d)

    def effective_efficiency(self, fiber_transmission: float) -> float:
        """
        End-to-end probability of a successful detection per sent photon:
        η_eff = η × T
        """
        return self.eta * fiber_transmission

    def max_clock_rate_hz(self) -> float:
        """
        Maximum sustainable clock rate before dead time causes >50% blocking.
        """
        if self.dead_time_ns == 0:
            return float("inf")
        return 1e9 / self.dead_time_ns

    def reset_counters(self) -> None:
        """Reset diagnostic counters at the start of a new session."""
        self._n_photon_detections = 0
        self._n_dark_detections   = 0
        self._n_missed_photons    = 0
        self._n_dead_time_blocks  = 0
        self._last_click_ns       = -self.dead_time_ns - 1.0

    def counters(self) -> dict:
        total = (
            self._n_photon_detections
            + self._n_dark_detections
            + self._n_missed_photons
            + self._n_dead_time_blocks
        )
        return {
            "photon_detections": self._n_photon_detections,
            "dark_detections":   self._n_dark_detections,
            "missed_photons":    self._n_missed_photons,
            "dead_time_blocks":  self._n_dead_time_blocks,
            "total_attempts":    total,
            "dark_fraction":     (
                self._n_dark_detections / max(1, self._n_photon_detections + self._n_dark_detections)
            ),
        }

    def describe(self) -> dict:
        return {
            "model":               "single_photon_detector",
            "eta":                 self.eta,
            "dark_count_hz":       self.dark_count_hz,
            "dark_prob_per_pulse": self._dark_prob_per_pulse,
            "dead_time_ns":        self.dead_time_ns,
            "clock_rate_hz":       self.clock_rate_hz,
            "max_clock_rate_hz":   self.max_clock_rate_hz(),
        }

    def __repr__(self) -> str:
        return (
            f"SinglePhotonDetector("
            f"η={self.eta}, "
            f"dark={self.dark_count_hz} Hz, "
            f"dead={self.dead_time_ns} ns)"
        )