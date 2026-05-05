import os
import httpx
import logging
from workers.celery_config import celery_app

logger  = logging.getLogger("worker.notify")
ORCH_URL = os.getenv("ORCH_URL", "http://localhost:8000")


@celery_app.task(
    bind=True,
    name="workers.notify_tasks.notify_orchestrator_task",
    queue="orchestrator",
    max_retries=5,
    default_retry_delay=2.0,
)
def notify_orchestrator_task(self, pipeline_result: dict) -> None:
    session_id = pipeline_result["session_id"]

    try:
        with httpx.Client(timeout=10.0) as client:
            # POST to new canonical route; /session/{id}/complete (legacy) is also
            # kept as an alias on the orchestrator so old worker images still work.
            resp = client.post(
                f"{ORCH_URL}/sessions/{session_id}/complete",
                json=pipeline_result,
            )
            resp.raise_for_status()
        logger.info(f"[notify] Session {session_id} notified → orchestrator")

    except httpx.HTTPError as e:
        logger.warning(f"[notify] Retry notification {session_id}: {e}")
        raise self.retry(exc=e)