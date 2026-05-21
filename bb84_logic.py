import random
from typing import Optional
from models import Basis, MeasurementRecord

QBER_THRESHOLD = 0.11
SAMPLE_FRACTION = 0.20


def compute_qber(
    alice_sifted: list[int],
    bob_sifted: list[int],
    sample_fraction: float = SAMPLE_FRACTION,
    sample_seed: Optional[int] = None,
) -> tuple[float, list[int], list[int]]:
   
    
    n = min(len(alice_sifted), len(bob_sifted))
    
    if len(alice_sifted) != len(bob_sifted):
        raise ValueError("Alice/Bob sifted mismatch  desync detected")
    if n == 0:
        return 1.0, [], []

    rng = random.Random(sample_seed)
    n_sample = max(1, int(n * sample_fraction))
    sample_indices = set(rng.sample(range(n), n_sample))

    errors = sum(
        1 for i in sample_indices
        if alice_sifted[i] != bob_sifted[i]
    )
    qber = errors / n_sample

    alice_key = [b for i, b in enumerate(alice_sifted) if i not in sample_indices]
    bob_key   = [b for i, b in enumerate(bob_sifted)   if i not in sample_indices]

    return qber, alice_key, bob_key


def perform_sifting_by_id(
    alice_bits: list[int],
    alice_bases: list[str],
    bob_measurements:dict[int, MeasurementRecord],
) -> tuple[list[int], list[int], list[int]]:

    alice_sifted = []
    bob_sifted = []
    matched_ids = []

    for qid in sorted(bob_measurements.keys()):
        if qid >=len(alice_bases):
            continue
        meas=bob_measurements[qid]
        if Basis(alice_bases[qid]) == meas.basis:
            alice_sifted.append(alice_bits[qid])
            bob_sifted.append(meas.bit_result)
            matched_ids.append(qid)

    return alice_sifted, bob_sifted, matched_ids
