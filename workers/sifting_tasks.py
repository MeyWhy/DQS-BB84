import hashlib
import logging
import os
import random
import time

import httpx
from optical.metrics import decompose_qber, estimate_key_rate

from workers.celery_config import celery_app
from kme.session_store import load_alice_state, delete_alice_state
from bb84_logic import compute_qber, QBER_THRESHOLD

logger  = logging.getLogger("worker.sifting")

KME_URL = os.getenv("KME_URL", "http://localhost:8000")

#Mirror Bob's deadline constants so ST waits at least as long as Bob does
QKDL_SECS_PER_QUBIT = 8.0
QKDL_FIXED_OVERHEAD  = 30.0
ST_POLL_INTERVAL     = 1.0   #seconds between KME polls while waiting for Bob



#ST  Sifting Task
@celery_app.task(
    name="workers.sifting_tasks.assemble_and_sift_task",
    queue="sifting",
)
def assemble_and_sift_task(batch_results: list[dict], session_meta: dict) -> dict:

    session_id = session_meta["session_id"]
    n_qubits   = session_meta.get("n_qubits", 0)
    kme_url    = session_meta.get("kme_url", KME_URL).rstrip("/")

    #1\ Aggregate delivered qubit IDs from QTT results 
    delivered_ids: set[int] = set()
    for br in batch_results:
        if not br:
            continue
        delivered_ids.update(br.get("delivered", []))
    n_delivered = len(delivered_ids)

    logger.info(
        f"[ST] session={session_id[:8]} "
        f"qtt_delivered={n_delivered}/{n_qubits}  waiting for Bob"
    )

    #2\ Wait for Bob to finish measuring and post to KME 
    #Deadline matches Bob's own poll deadline so we never give up before Bob.
    deadline = time.time() + QKDL_FIXED_OVERHEAD + n_qubits * QKDL_SECS_PER_QUBIT
    raw_meas = []

    while time.time() < deadline:
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.get(
                    f"{kme_url}/sessions/{session_id}/measurements"
                )
                resp.raise_for_status()
                raw_meas = resp.json().get("measurements", [])
        except Exception as e:
            logger.warning(
                f"[ST] Measurement poll error session={session_id[:8]}: {e}"
            )
            time.sleep(ST_POLL_INTERVAL)
            continue

        if raw_meas:
            logger.info(
                f"[ST] Measurements ready session={session_id[:8]} "
                f"n={len(raw_meas)}"
            )
            break

        #Not ready yet  log progress every 10 polls
        waited = (
            QKDL_FIXED_OVERHEAD + n_qubits * QKDL_SECS_PER_QUBIT
            - (deadline - time.time())
        )
        if int(waited) % 10 == 0:
            logger.debug(
                f"[ST] Still waiting for Bob session={session_id[:8]} "
                f"waited={waited:.0f}s"
            )
        time.sleep(ST_POLL_INTERVAL)

    if not raw_meas:
        logger.error(
            f"[ST] Timeout waiting for measurements session={session_id[:8]}"
        )
        return _abort(session_id, n_delivered, n_qubits, "BOB_TIMEOUT", kme_url)

    #3\ Load Alice's bits+bases from Redis 
    alice_state = load_alice_state(session_id)
    if not alice_state:
        logger.error(
            f"[ST] Alice state missing from Redis session={session_id[:8]}"
        )
        return _abort(session_id, n_delivered, n_qubits, "ALICE_STATE_MISSING",
                      kme_url)

    alice_bits  = alice_state["bits"]
    alice_bases = alice_state["bases"]   #list of "Z" / "X" strings

    #4\ Basis reconciliation by qubit_id 
    meas_by_id    = {m["qubit_id"]: m for m in raw_meas}
    alice_sifted: list[int] = []
    bob_sifted:   list[int] = []

    for qid in sorted(meas_by_id.keys()):
        if qid >= len(alice_bases):
            continue
        m = meas_by_id[qid]
        if alice_bases[qid] == m.get("basis"):
            alice_sifted.append(alice_bits[qid])
            bob_sifted.append(m.get("bit_result", 0))

    n_sifted    = len(alice_sifted)
    sample_seed = random.randint(0, 2**31)

    logger.info(
        f"[ST] session={session_id[:8]} n_sifted={n_sifted}/{n_qubits}"
    )

    #5\ Post Alice's bases to KME so Bob can do his local sift 
    alice_bases_payload = [
        (qid, alice_bases[qid])
        for qid in sorted(meas_by_id.keys())
        if qid < len(alice_bases)
    ]
    try:
        with httpx.Client(timeout=10.0) as client:
            client.post(
                f"{kme_url}/sessions/{session_id}/sift",
                json={
                    "session_id":  session_id,
                    "alice_bases": alice_bases_payload,
                    "sample_seed": sample_seed,
                },
            )
    except Exception as e:
        logger.warning(f"[ST] Post sift failed session={session_id[:8]}: {e}")

    return {
        "session_id":   session_id,
        "error":        None,
        "alice_sifted": alice_sifted,
        "bob_sifted":   bob_sifted,
        "n_sifted":     n_sifted,
        "n_delivered":  n_delivered,
        "n_qubits":     n_qubits,
        "sample_seed":  sample_seed,
        "kme_url":      kme_url,
        "qkdl_url":     session_meta.get("qkdl_url", "http://localhost:8003"),  #← ADD

    }


def _abort(session_id, n_delivered, n_qubits, reason, kme_url=None) -> dict:
    return {
        "session_id":   session_id,
        "error":        reason,
        "alice_sifted": [],
        "bob_sifted":   [],
        "n_sifted":     0,
        "n_delivered":  n_delivered,
        "n_qubits":     n_qubits,
        "sample_seed":  0,
        "kme_url":      kme_url or KME_URL,
    }



#QKT:  QBER & Key Derivation Task
@celery_app.task(
    name="workers.sifting_tasks.qber_key_task",
    queue="sifting",
)
def qber_key_task(sifting_result: dict) -> dict:
    session_id = sifting_result["session_id"]
    kme_url    = sifting_result.get("kme_url", KME_URL).rstrip("/")
    qkdl_url   = sifting_result.get("qkdl_url", "http://localhost:8003") 
    
    if sifting_result.get("error"):
        logger.warning(
            f"[QKT] Upstream error session={session_id[:8]}: "
            f"{sifting_result['error']}"
        )
        return {
            "session_id":    session_id,
            "status":        "aborted",
            "error_message": sifting_result["error"],
            "qber":          1.0,
            "key_final":     "",
            "key_hash":      "",
            "n_sifted":      0,
            "n_delivered":   sifting_result.get("n_delivered", 0),
            "n_qubits":      sifting_result.get("n_qubits", 0),
            "kme_url":       kme_url,
            "qkdl_url": qkdl_url,
        }

    alice_sifted = sifting_result["alice_sifted"]
    bob_sifted   = sifting_result["bob_sifted"]
    sample_seed  = sifting_result["sample_seed"]
    n_sifted     = sifting_result["n_sifted"]
    n_delivered  = sifting_result["n_delivered"]
    n_qubits     = sifting_result["n_qubits"]

    if n_sifted < 10:
        logger.warning(
            f"[QKT] Insufficient bits session={session_id[:8]} n={n_sifted}"
        )
        return {
            "session_id":    session_id,
            "status":        "aborted",
            "error_message": "INSUFFICIENT_BITS",
            "qber":         0.0,
            "key_final":     "",
            "key_hash":      "",
            "n_sifted":      n_sifted,
            "n_delivered":   n_delivered,
            "n_qubits":      n_qubits,
            "kme_url":       kme_url,
            "qkdl_url": qkdl_url,
        }

    try:
        qber, alice_final, _ = compute_qber(
            alice_sifted, bob_sifted, sample_seed=sample_seed
        )
    except ValueError as e:
        logger.error(f"[QKT] compute_qber failed session={session_id[:8]}: {e}")
        return {
            "session_id":    session_id,
            "status":        "aborted",
            "error_message": f"QBER_COMPUTE_ERROR: {e}",
            "qber":          1.0,
            "key_final":     "",
            "key_hash":      "",
            "n_sifted":      n_sifted,
            "n_delivered":   n_delivered,
            "n_qubits":      n_qubits,
            "kme_url":       kme_url,
            "qkdl_url": qkdl_url,
        }
    

    logger.info(
        f"[QKT] session={session_id[:8]} QBER={qber*100:.2f}% "
        f"threshold={QBER_THRESHOLD*100:.1f}%"
    )
    #In qber_key_task, after computing qber:

    #Fetch physical floor from QKDL channel status
    physical_floor = 0.0

    try:
        r = httpx.get(f"{qkdl_url}/channel/status/{session_id}", timeout=3.0)
        if r.status_code == 200:
            physical_floor = r.json().get("qber_budget", {}).get("physical_floor", 0.0)
    except Exception:
        pass

    qber_analysis = decompose_qber(qber, physical_floor)

    #Log the decomposition
    logger.info(
        f"[QKT] QBER analysis session={session_id[:8]}: "
        f"measured={qber_analysis['measured']*100:.3f}% "
        f"physical={qber_analysis['physical']*100:.3f}% "
        f"eve_estimate={qber_analysis['eve_estimate']*100:.3f}% "
        f"confidence={qber_analysis['confidence']}"
    )

    #Include in the key upload payload (extend your existing KeyUpload call)
    #Add to the metadata / error_message field or extend the model if needed
    if qber > QBER_THRESHOLD:
        logger.warning(
            f"[QKT] QBER_TOO_HIGH session={session_id[:8]} "
            f"QBER={qber*100:.2f}%"
        )
        return {
            "session_id":    session_id,
            "status":        "aborted",
            "error_message": "QBER_TOO_HIGH",
            "qber":          qber,
            "key_final":     "",
            "key_hash":      "",
            "n_sifted":      n_sifted,
            "n_delivered":   n_delivered,
            "n_qubits":      n_qubits,
            "kme_url":       kme_url,
            "qkdl_url": qkdl_url,
        }

    key_final = "".join(map(str, alice_final))
    key_hash  = hashlib.sha256(bytes(alice_final)).hexdigest()

    logger.info(
        f"[QKT] Key derived session={session_id[:8]} "
        f"key_len={len(alice_final)} QBER={qber*100:.2f}%"
    )

    #Alice's bits/bases no longer needed  clean up Redis
    delete_alice_state(session_id)

    return {
        "session_id":    session_id,
        "status":        "success",
        "error_message": "",
        "qber":          qber,
        "key_final":     key_final,
        "key_hash":      key_hash,
        "n_sifted":      n_sifted,
        "n_delivered":   n_delivered,
        "n_qubits":      n_qubits,
        "kme_url":       kme_url,
        "qkdl_url": qkdl_url,
    }



#NT  Notification Task
@celery_app.task(
    bind=True,
    name="workers.sifting_tasks.notify_kme_task",
    queue="sifting",
    max_retries=5,
    default_retry_delay=2.0,
)
def notify_kme_task(self, pipeline_result: dict) -> None:
    session_id = pipeline_result["session_id"]
    kme_url    = pipeline_result.get("kme_url", KME_URL).rstrip("/")
    node_id    = pipeline_result.get("node_id", "worker")

    payload = {
        "session_id":    session_id,
        "node_id":       node_id,
        "key_final":     pipeline_result.get("key_final", ""),
        "key_hash":      pipeline_result.get("key_hash", ""),
        "qber":          pipeline_result.get("qber", 0.0),
        "n_sifted":      pipeline_result.get("n_sifted", 0),
        "status":        pipeline_result.get("status", "aborted"),
        "error_message": pipeline_result.get("error_message", ""),
    }

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                f"{kme_url}/sessions/{session_id}/key",
                json=payload,
            )
            resp.raise_for_status()
        logger.info(
            f"[NT] KME notified session={session_id[:8]} "
            f"status={payload['status']} QBER={payload['qber']*100:.2f}%"
        )
    except httpx.HTTPError as e:
        logger.warning(
            f"[NT] KME notify failed session={session_id[:8]}: {e}  retrying"
        )
        raise self.retry(exc=e)