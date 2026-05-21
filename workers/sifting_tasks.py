import os
import logging
import httpx

from workers.celery_config import celery_app

logger = logging.getLogger("worker.sifting")


@celery_app.task(
    name="workers.sifting_tasks.batch_complete_task",
    queue="sifting",
)
def batch_complete_task(batch_results: list[dict], session_meta: dict) -> dict:

    session_id        = session_meta["session_id"]
    alice_callback    = session_meta["alice_callback_url"]
    n_qubits          = session_meta.get("n_qubits", 0)

    # Aggregate delivery across all batches
    delivered_ids: set[int] = set()
    failed_ids:    set[int] = set()

    for batch_res in batch_results:
        if not batch_res:
            continue
        delivered_ids.update(batch_res.get("delivered", []))
        failed_ids.update(batch_res.get("failed", []))

    n_delivered = len(delivered_ids)
    n_failed    = len(failed_ids)

    logger.info(
        f"[chord] Session {session_id[:8]}  "
        f"all batches done: delivered={n_delivered}/{n_qubits} "
        f"failed={n_failed}"
    )

    # Notify Alice that all qubits have been sent.
    # Alice's /webhook endpoint dispatches on event name.
    # We use "transmission_complete"  Alice handles it in on_transmission_complete().
    # This is a direct POST to Alice's callback URL (not via KME) so it fires
    # even if Bob hasn't finished measuring yet.
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                alice_callback,
                json={
                    "event":      "transmission_complete",
                    "session_id": session_id,
                    "payload": {
                        "n_delivered": n_delivered,
                        "n_failed":    n_failed,
                        "n_qubits":    n_qubits,
                    },
                },
            )
            resp.raise_for_status()
            logger.info(
                f"[chord] Alice notified session={session_id[:8]} "
                f"n_delivered={n_delivered}"
            )
    except Exception as e:
        logger.error(
            f"[chord] Alice notification failed session={session_id[:8]}: {e}"
        )
        # Non-fatal: Alice's poll_tick will eventually pick up measurements
        # from KME if the direct notification fails.

    return {
        "session_id":  session_id,
        "n_delivered": n_delivered,
        "n_failed":    n_failed,
        "status":      "complete",
    }
