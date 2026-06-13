
from __future__ import annotations
import math
import random
import logging

logger = logging.getLogger("optical.channel")

from .polarization import PolarizationDriftChannel, OUDriftChannel
from .detector import SinglePhotonDetector

#Physical polarization encoding table - shared by both channel classes
_ENCODE: dict[tuple[str, int], str] = {
    ("Z", 0): "H",
    ("Z", 1): "V",
    ("X", 0): "D",
    ("X", 1): "A",
}
_DECODE: dict[str, tuple[str, int]] = {v: k for k, v in _ENCODE.items()}


class FiberChannel:

    def __init__(
        self,
        distance_km:     float,
        alpha_db_per_km: float = 0.2,
        enable_drift:    bool  = True,
        enable_detector: bool  = True,
        use_ou_drift:    bool  = True,
        eta:             float = 0.85,
        dark_count_hz:   float = 100.0,
        dead_time_ns:    float = 50.0,
        #Ansys CSV source (either a path or a pre-built model)
        csv_path:        str | None = None,
        channel_model=   None,   #ChannelModel instance or None
    ):
        if distance_km < 0:
            raise ValueError(f"distance_km must be >= 0, got {distance_km}")

        self.distance_km     = distance_km
        self.alpha_db_per_km = alpha_db_per_km

        #choose transmission source
        self._channel_model = None
        if channel_model is not None:
            self._channel_model = channel_model
            self._transmission  = channel_model.transmission_prob(distance_km)
        elif csv_path is not None:
            from .channel_model import ChannelModel
            self._channel_model = ChannelModel(csv_path)
            self._transmission  = self._channel_model.transmission_prob(distance_km)
        else:
            #Analytical fallback (same formula as before)
            self._transmission = 10 ** (-(alpha_db_per_km * distance_km) / 10)

        #polarization drift (single assignment)
        if enable_drift and distance_km > 0:
            if use_ou_drift:
                self.drift = OUDriftChannel.from_distance(distance_km)
            else:
                self.drift = PolarizationDriftChannel.from_distance(distance_km)
        else:
            self.drift = None

        #single-photon detector
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
        #attenuation (formula or Ansys CSV)
        survived = None
        if photon is not None:
            if random.random() <= self._transmission:
                survived = photon

        #olarization drift
        if survived is not None and self.drift is not None:
            basis_val = survived.get("basis")
            bit       = survived.get("bit")
            if basis_val is not None and bit is not None:
                phys_state = _ENCODE.get((basis_val, int(bit)))
                if phys_state:
                    drifted_state = self.drift.apply(phys_state)
                    if drifted_state != phys_state:
                        new_basis, new_bit = _DECODE[drifted_state]
                        survived = {**survived, "basis": new_basis, "bit": new_bit}

        #detector
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
        drift_qber    = self.drift.qber_contribution() if self.drift else 0.0
        detector_qber = (
            self.detector.qber_contribution(self._transmission)
            if self.detector else 0.0
        )
        #PMD floor from ChannelModel (CSV - 0.0 until then)
        pmd_floor = (
            self._channel_model.qber_floor(self.distance_km)
            if self._channel_model else 0.0
        )
        return drift_qber + detector_qber + pmd_floor

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
            "loss_db":           round(self.alpha_db_per_km * self.distance_km, 3),
            "qber_floor":        self.qber_floor(),
            "ansys_csv_loaded":  self._channel_model is not None,
        }
        if self._channel_model:
            d["channel_model"] = self._channel_model.describe(self.distance_km)
        if self.drift:
            d["polarization_drift"] = self.drift.describe()
        if self.detector:
            d["detector"] = self.detector.describe()
        return d


class StatisticalChannel:
    #probabilistic loss model
    def __init__(self, loss_rate: float = 0.0):
        if not 0.0 <= loss_rate <= 1.0:
            raise ValueError(f"loss_rate must be in [0, 1], got {loss_rate}")
        self.loss_rate = loss_rate

    def transmit(self, photon: dict | None, t_ns: float = 0.0) -> dict | None:
        if photon is None:
            return None
        if self.loss_rate > 0.0 and random.random() < self.loss_rate:
            return None
        return photon

    def transmission_probability(self) -> float:
        return 1.0 - self.loss_rate

    def qber_floor(self) -> float:
        return 0.0

    def reset_session(self) -> None:
        pass

    def describe(self) -> dict:
        return {
            "model":             "statistical",
            "loss_rate":         self.loss_rate,
            "transmission_prob": self.transmission_probability(),
            "qber_floor":        self.qber_floor(),
        }

    def __repr__(self) -> str:
        return f"StatisticalChannel(loss_rate={self.loss_rate})"