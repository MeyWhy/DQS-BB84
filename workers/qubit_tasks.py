
import os
import logging
import httpx

from workers.celery_config import celery_app
from models import QubitBatch

logger  = logging.getLogger("worker.qubit")

#Celery task: send one qubit batch to the QKDL.
#called N times in a chord by Alice (one task per batch).
@celery_app.task(
    bind=True,
    name="workers.qubit_tasks.send_batch_task",
    max_retries=2,
    default_retry_delay=1.0,
    queue="qubit_send",
)
def send_batch_task(self, session_id: str, batch_payload: dict, qkdl_url: str) -> dict:
    qkdl_url = qkdl_url.rstrip("/")

    try:
        with httpx.Client(timeout=120.0) as client:
            resp = client.post(
                f"{qkdl_url}/batch/send",
                json={"session_id": session_id, "batch": batch_payload},
            )
            resp.raise_for_status()
            data = resp.json()

        results   = data.get("results") or []
        delivered = [r["qubit_id"] for r in results if r.get("delivered")]
        failed    = [r["qubit_id"] for r in results if not r.get("delivered")]

        logger.debug(
            f"[batch {data.get('batch_id')}] session={session_id[:8]} "
            f"delivered={len(delivered)} failed={len(failed)} qkdl={qkdl_url}"
        )

        return {
            "session_id": session_id,
            "batch_id":   data.get("batch_id"),
            "delivered":  delivered,
            "failed":     failed,
        }

    except httpx.HTTPStatusError as e:
        logger.warning(
            f"[batch] HTTP {e.response.status_code} "
            f"session={session_id[:8]}  retry {self.request.retries}"
        )
        raise self.retry(exc=e)

    except httpx.RequestError as e:
        logger.warning(
            f"[batch] QKDL unreachable session={session_id[:8]}: {e}"
        )
        raise self.retry(exc=e)

    except Exception as e:
        logger.error(
            f"[batch] Unexpected error session={session_id[:8]}: {e}"
        )
        # Return a failed record so the chord still completes
        batch    = QubitBatch.model_validate(batch_payload)
        batch_id = batch.batch_id
        return {
            "session_id": session_id,
            "batch_id":   batch_id,
            "delivered":  [],
            "failed":     [q.qubit_id for q in batch.qubits],
        }
