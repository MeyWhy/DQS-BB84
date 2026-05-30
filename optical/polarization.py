from __future__ import annotations
import logging
import math
import random

logger = logging.getLogger("optical.polarization")

# BB84 polarization angles on the Poincaré equator (radians)
_STATE_ANGLE: dict[str, float] = {
    "H": 0.0,
    "D": math.pi / 4,
    "V": math.pi / 2,
    "A": 3 * math.pi / 4,
}

# Inverse map: angle → nearest BB84 state
_ANGLE_STATE: list[tuple[float, str]] = sorted(
    (_STATE_ANGLE[s], s) for s in _STATE_ANGLE
)


def _nearest_state(angle: float) -> str:
    """
    Project a rotated angle back to the nearest BB84 polarization state.
    Wraps at π (H and V are π apart; D and A are π apart).
    """
    angle = angle % math.pi
    best, best_dist = "H", math.inf
    for ref_angle, state in _ANGLE_STATE:
        dist = abs(angle - ref_angle)
        dist = min(dist, math.pi - dist)   # wrap-around distance
        if dist < best_dist:
            best_dist = dist
            best      = state
    return best


class PolarizationDriftChannel:
    """
    Step 2 — Static Gaussian polarization rotation noise.

    Models birefringence in optical fiber as a single Gaussian rotation
    applied independently to each photon.  This is the simplest physically
    grounded model: each photon accumulates a random phase shift drawn
    from N(0, σ²) where σ grows with fiber length.

    Physics note
    ------------
    Real fiber birefringence is a random walk on the Poincaré sphere.
    A single Gaussian rotation on the equator is the projection of that
    walk onto the polarization plane — accurate enough for step 2, and
    it becomes the OU process in Step 4.

    Parameters
    ----------
    sigma_deg : float
        Standard deviation of the rotation noise in degrees.
        Typical values:
          - Lab patch cable (~1 m):   0.1–0.5°
          - 10 km SMF:                1–3°
          - 50 km SMF:                3–6°
          - 100 km SMF:               5–12°
        Rule of thumb from distance: sigma_deg ≈ 0.5 + 0.08 * distance_km
    """

    def __init__(self, sigma_deg: float):
        if sigma_deg < 0:
            raise ValueError(f"sigma_deg must be >= 0, got {sigma_deg}")
        self.sigma_deg = sigma_deg
        self._sigma_rad = math.radians(sigma_deg)

    @classmethod
    def from_distance(cls, distance_km: float) -> "PolarizationDriftChannel":
        """
        Derive sigma_deg from fiber length using a linear empirical model.
        This is the recommended constructor when distance is already known.
        """
        sigma_deg = 0.5 + 0.08 * distance_km
        return cls(sigma_deg)

    def apply(self, state: str) -> str:
        """
        Apply a random Gaussian rotation to a BB84 polarization state.

        Parameters
        ----------
        state : str
            One of "H", "V", "D", "A".

        Returns
        -------
        str
            The nearest BB84 state after rotation.
            May differ from input — that difference is a QBER contribution.
        """
        if state not in _STATE_ANGLE:
            raise ValueError(
                f"Unknown polarization state '{state}'. "
                f"Expected one of {list(_STATE_ANGLE)}"
            )

        theta = random.gauss(0.0, self._sigma_rad)
        rotated = _STATE_ANGLE[state] + theta
        result  = _nearest_state(rotated)

        if result != state:
            logger.debug(
                "Polarization flip: %s → %s (rotation=%.3f°)",
                state, result, math.degrees(theta),
            )

        return result

    def qber_contribution(self) -> float:
        """
        Analytical QBER floor from polarization drift alone.

        P(flip) = erfc(π/8 / (σ√2))

        The threshold π/8 is the half-angle between adjacent BB84 states
        (states are spaced π/4 apart; a flip occurs when |θ| exceeds π/8).
        math.erfc() is numerically stable for all z, including z >> 3
        where the manual approximation previously underflowed to 0.
        """
        if self._sigma_rad == 0:
            return 0.0
        z = (math.pi / 8) / (self._sigma_rad * math.sqrt(2))
        return math.erfc(z)
    
    def describe(self) -> dict:
        return {
            "model":              "polarization_drift_gaussian",
            "sigma_deg":          self.sigma_deg,
            "sigma_rad":          round(self._sigma_rad, 6),
            "qber_contribution":  round(self.qber_contribution(), 6),
        }

    def __repr__(self) -> str:
        return (
            f"PolarizationDriftChannel("
            f"sigma_deg={self.sigma_deg:.2f}, "
            f"qber_floor={self.qber_contribution():.4f})"
        )
    
class OUDriftChannel:
    """
    Step 4 — Ornstein-Uhlenbeck time-varying polarization drift.

    Replaces the per-photon independent Gaussian of PolarizationDriftChannel
    with a correlated random walk. The channel state θ evolves over time,
    producing realistic QBER bursts instead of a flat noise floor.

    Parameters
    ----------
    kappa : float
        Mean-reversion rate (1/s). Higher = faster recovery to zero drift.
        Typical fiber: 0.01–0.1 (slow thermal drift).
    sigma : float
        Noise intensity (rad/√s). Controls how far the drift wanders.
    dt : float
        Time step in seconds per qubit. At 1 MHz clock: dt = 1e-6.
    theta0 : float
        Initial drift angle in radians (default 0 = no initial drift).
    """

    def __init__(
        self,
        kappa:  float = 0.01,
        sigma:  float = 0.05,
        dt:     float = 1e-6,
        theta0: float = 0.0,
    ):
        self.kappa  = kappa
        self.sigma  = sigma
        self.dt     = dt
        self.theta  = theta0   # current drift angle (radians)
        self._history: list[float] = []   # for diagnostics

    @classmethod
    def from_distance(cls, distance_km: float) -> "OUDriftChannel":
        """
        Scale OU noise intensity with fiber length.
        Longer fiber = more birefringence variance per unit time.
        """
        sigma = 0.01 + 0.001 * distance_km   # empirical scaling
        return cls(kappa=0.02, sigma=sigma, dt=1e-6)

    def step(self) -> float:
        """
        Advance the OU process by one time step.
        Returns the new drift angle in radians.
        """
        dW = random.gauss(0.0, 1.0)
        self.theta += (
            -self.kappa * self.theta * self.dt
            + self.sigma * (self.dt ** 0.5) * dW
        )
        self._history.append(self.theta)
        return self.theta

    def apply(self, phys_state: str) -> str:
        """
        Apply current OU drift angle to a physical polarization state.
        Calls step() internally — one step per photon.
        """
        self.step()
        rotated = _STATE_ANGLE[phys_state] + self.theta
        return _nearest_state(rotated)

    def current_qber_contribution(self) -> float:
        """Instantaneous QBER from current drift angle."""
        import math
        return math.sin(self.theta / 2) ** 2

    def session_qber_estimate(self) -> float:
        """
        Mean QBER contribution over the session so far,
        estimated from the drift history.
        """
        import math
        if not self._history:
            return 0.0
        return sum(math.sin(t / 2) ** 2 for t in self._history) / len(self._history)

    def reset(self) -> None:
        self.theta = 0.0
        self._history.clear()

    def describe(self) -> dict:
        return {
            "model":                  "ou_drift",
            "kappa":                  self.kappa,
            "sigma":                  self.sigma,
            "dt":                     self.dt,
            "current_theta_rad":      round(self.theta, 6),
            "current_theta_deg":      round(math.degrees(self.theta), 4),
            "current_qber":           round(self.current_qber_contribution(), 8),
            "session_qber_estimate":  round(self.session_qber_estimate(), 8),
            "n_steps":                len(self._history),
        }