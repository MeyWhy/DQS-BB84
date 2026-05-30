from __future__ import annotations
import random
import logging

logger = logging.getLogger("optical.channel")

from .polarization import PolarizationDriftChannel, OUDriftChannel
from .detector import SinglePhotonDetector

# Add this mapping at module level in channel.py, above FiberChannel
# Maps (basis_value, bit) → physical polarization state
_ENCODE: dict[tuple[str, int], str] = {
    ("Z", 0): "H",   # rectilinear, bit 0 → horizontal
    ("Z", 1): "V",   # rectilinear, bit 1 → vertical
    ("X", 0): "D",   # diagonal, bit 0 → diagonal
    ("X", 1): "A",   # diagonal, bit 1 → anti-diagonal
}

# Reverse: physical state → (basis_value, bit)
_DECODE: dict[str, tuple[str, int]] = {v: k for k, v in _ENCODE.items()}

class FiberChannel:
    def __init__(
        self,
        distance_km:     float,
        alpha_db_per_km: float = 0.2,
        enable_drift:    bool  = True,
        enable_detector: bool  = True,    # <- Step 3 toggle
        use_ou_drift:    bool  = True,  # <- step 4
        eta:             float = 0.85,
        dark_count_hz:   float = 100.0,
        dead_time_ns:    float = 50.0,
    ):
        if distance_km < 0:
            raise ValueError(f"distance_km must be >= 0, got {distance_km}")

        self.distance_km     = distance_km
        self.alpha_db_per_km = alpha_db_per_km
        self._transmission   = 10 ** (-(alpha_db_per_km * distance_km) / 10)

        self.drift = (
            PolarizationDriftChannel.from_distance(distance_km)
            if enable_drift and distance_km > 0
            else None
        )
        if enable_drift and distance_km > 0:
            if use_ou_drift:
                self.drift = OUDriftChannel.from_distance(distance_km)
            else:
                self.drift = PolarizationDriftChannel.from_distance(distance_km)
        else:
            self.drift = None
        
        # Step 3: Bob's detector — shared across all photons in a session
        self.detector = (
            SinglePhotonDetector(
                eta=eta,
                dark_count_hz=dark_count_hz,
                dead_time_ns=dead_time_ns,
            )
            if enable_detector
            else None
        )

    def transmit(self, photon: dict | None, t_ns: float = 0.0) -> dict | None:
        # Step 1 — attenuation
        survived = None
        if photon is not None:
            if random.random() <= self._transmission:
                survived = photon

        # Step 2 — polarization drift on the PHYSICAL STATE, not the basis label
        if survived is not None and self.drift is not None:
            basis_val = survived.get("basis")   # "Z" or "X"
            bit       = survived.get("bit")     # 0 or 1

            if basis_val is not None and bit is not None:
                phys_state = _ENCODE.get((basis_val, int(bit)))
                if phys_state:
                    drifted_state = self.drift.apply(phys_state)
                    if drifted_state != phys_state:
                        # Decode back to (basis, bit) for the rest of the pipeline
                        new_basis, new_bit = _DECODE[drifted_state]
                        survived = {**survived, "basis": new_basis, "bit": new_bit}

        # Step 3 — detector
        if self.detector is not None:
            clicked, reason = self.detector.detect(survived, t_ns)
            if not clicked:
                return None
            if reason == "dark":
                return {
                    "dark_count": True,
                    "basis":      None,
                    "bit":        None,
                    "qubit_id":   photon.get("qubit_id") if photon else None,
                }
            return survived

        return survived
    def qber_floor(self) -> float:
        """
        Combined physical QBER floor from drift + dark counts.
        """
        drift_qber    = self.drift.qber_contribution() if self.drift else 0.0
        detector_qber = (
            self.detector.qber_contribution(self._transmission)
            if self.detector else 0.0
        )
        # Independent contributions — add linearly (both are small)
        return drift_qber + detector_qber

    def reset_session(self) -> None:
        if self.detector:
            self.detector.reset_counters()
        if self.drift and hasattr(self.drift, "reset"):
            self.drift.reset()  

    def describe(self) -> dict:
        d = {
            "model":             "fiber_attenuation",
            "distance_km":       self.distance_km,
            "alpha_db_per_km":   self.alpha_db_per_km,
            "transmission_prob": self._transmission,
            "loss_db":           self.alpha_db_per_km * self.distance_km,
            "qber_floor":        self.qber_floor(),
        }
        if self.drift:
            d["polarization_drift"] = self.drift.describe()
        if self.detector:
            d["detector"] = self.detector.describe()
        return d
    
class StatisticalChannel:
    """
    Step 0 — probabilistic loss model.

    This is a direct refactor of the inline loss logic that currently
    lives inside _process_batch_sync(). Extracting it here means:
      - qunetsim_service.py stops knowing about loss details
      - you can swap StatisticalChannel → FiberChannel (Step 1)
        in one line without touching any session or batch logic
    """

    def __init__(self, loss_rate: float = 0.0):
        if not 0.0 <= loss_rate <= 1.0:
            raise ValueError(f"loss_rate must be in [0, 1], got {loss_rate}")
        self.loss_rate = loss_rate

    def transmit(self, photon: dict | None, t_ns: float=0.0) -> dict | None:
        """
        Attempt to transmit a photon through the channel.

        Parameters
        ----------
        photon : dict | None
            Arbitrary photon record (qubit_id, bit, basis, …).
            Passing None is a no-op — returns None immediately.

        Returns
        -------
        dict | None
            The original photon dict if it survived, None if lost.
        """
        if photon is None:
            return None
        if self.loss_rate > 0.0 and random.random() < self.loss_rate:
            logger.debug(
                "Photon lost (loss_rate=%.3f, qubit_id=%s)",
                self.loss_rate, photon.get("qubit_id"),
            )
            return None
        return photon

    # ------------------------------------------------------------------
    # Metrics helpers — Step 0 exposes almost nothing here,
    # but the interface is established so Steps 1–5 can override it.
    # ------------------------------------------------------------------

    def transmission_probability(self) -> float:
        """Fraction of photons expected to survive the channel."""
        return 1.0 - self.loss_rate

    def qber_floor(self) -> float:
        """
        Physical QBER contribution from this channel alone (no Eve).
        Step 0: pure loss doesn't add bit errors, so this is 0.
        Steps 2–4 will return non-zero values here.
        """
        return 0.0

    def describe(self) -> dict:
        """Structured summary for /health and logging."""
        return {
            "model":                "statistical",
            "loss_rate":            self.loss_rate,
            "transmission_prob":    self.transmission_probability(),
            "qber_floor":           self.qber_floor(),
        }

    def __repr__(self) -> str:
        return f"StatisticalChannel(loss_rate={self.loss_rate})"
