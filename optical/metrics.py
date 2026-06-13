from __future__ import annotations
"""
optical/metrics.py: QBER decomposition and key rate estimation.

Provides functions that translate raw session numbers into physically
interpretable quantities. Used by sifting workers and the KME.
"""
import math


def decompose_qber(
    measured_qber:    float,
    physical_floor:   float,
) -> dict:
  
    eve_estimate = max(0.0, measured_qber - physical_floor)

    #BB84 security threshold: Eve doing intercept-resend injects ~25% QBER.
    #With information-theoretic security, abort above 11%.
    #We flag suspicion above 5% excess (conservative for research use).
    SUSPICION_THRESHOLD = 0.05
    ABORT_THRESHOLD     = 0.11

    if measured_qber >= ABORT_THRESHOLD:
        confidence = "abort"
    elif eve_estimate >= SUSPICION_THRESHOLD:
        confidence = "suspicious"
    elif measured_qber > physical_floor * 3:
        confidence = "noisy"
    else:
        confidence = "clean"

    return {
        "measured":       round(measured_qber, 6),
        "physical":       round(physical_floor, 6),
        "eve_estimate":   round(eve_estimate, 6),
        "eve_detectable": eve_estimate >= SUSPICION_THRESHOLD,
        "confidence":     confidence,
    }


def estimate_key_rate(
    n_qubits:        int,
    transmission:    float,
    eta:             float = 0.85,
    qber:            float = 0.0,
    dark_prob:       float = 1e-4,
) -> dict:
    """
    Estimate theoretical key rate using a simplified BB84 model.

    Key rate (bits per sent qubit):
        R = q · η · T · (1 - H(QBER) - H(QBER))
    where q=0.5 (basis sifting), H is binary entropy, T is transmission.

    This is a conservative lower bound — no privacy amplification modelled.

    Returns
    -------
    dict with sifted_count estimate, key_rate, and projected_key_bits.
    """
    def h(p: float) -> float:
        """Binary entropy."""
        if p <= 0 or p >= 1:
            return 0.0
        return -p * math.log2(p) - (1 - p) * math.log2(1 - p)

    sifted_fraction = 0.5 * eta * transmission
    sifted_estimate = int(n_qubits * sifted_fraction)

    #After QBER sampling (20% of sifted bits sacrificed)
    key_fraction = sifted_fraction * 0.8

    #Secret key fraction after error correction and privacy amplification
    #Simplified: 1 - 2*H(QBER) (ignores finite-key effects)
    secret_fraction = max(0.0, 1.0 - 2 * h(qber)) if qber < 0.11 else 0.0

    projected_key_bits = int(n_qubits * key_fraction * secret_fraction)

    return {
        "n_qubits":            n_qubits,
        "transmission":        round(transmission, 6),
        "sifted_estimate":     sifted_estimate,
        "key_fraction":        round(key_fraction, 6),
        "secret_fraction":     round(secret_fraction, 6),
        "projected_key_bits":  projected_key_bits,
        "viable":              projected_key_bits > 0,
    }


def session_report(
    session_data:   dict,
    channel_describe: dict,
) -> dict:

    measured_qber  = session_data.get("qber", 0.0)
    physical_floor = channel_describe.get("qber_floor", 0.0)
    transmission   = channel_describe.get("transmission_prob", 1.0)
    n_qubits       = session_data.get("n_qubits", 0)
    n_delivered    = session_data.get("n_delivered", 0)
    n_sifted       = session_data.get("n_sifted", 0)

    qber_breakdown = decompose_qber(measured_qber, physical_floor)
    key_rate       = estimate_key_rate(
        n_qubits=n_qubits,
        transmission=transmission,
        qber=measured_qber,
    )

    #Actual delivery rate vs theoretical
    theoretical_delivery = int(n_qubits * transmission * 0.85)
    delivery_efficiency  = (
        round(n_delivered / theoretical_delivery, 3)
        if theoretical_delivery > 0 else 0.0
    )

    return {
        "session_id":          session_data.get("session_id", ""),
        "status":              session_data.get("status", ""),
        "distance_km":         channel_describe.get("distance_km", 0.0),
        "n_qubits":            n_qubits,
        "n_delivered":         n_delivered,
        "n_sifted":            n_sifted,
        "delivery_efficiency": delivery_efficiency,
        "qber_analysis":       qber_breakdown,
        "key_rate_model":      key_rate,
        "channel":             channel_describe,
    }