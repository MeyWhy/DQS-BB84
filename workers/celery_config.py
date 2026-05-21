import os
from celery import Celery

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "bb84",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    result_backend_transport_options={
        "retry_policy": {"timeout": 5.0}
    },
    task_routes={
        "workers.qubit_tasks.send_batch_task":        {"queue": "qubit_send"},
        "workers.sifting_tasks.batch_complete_task":  {"queue": "sifting"},
    },
    # Each worker pulls one task at a time  prevents a single worker
    # monopolising all qubit batches while others sit idle.
    worker_prefetch_multiplier=1,
    task_acks_late=True,   # ack after execution, not on receipt
    result_chord_join_timeout=300,
)

celery_app.autodiscover_tasks(["workers"])
celery_app.conf.imports = (
    "workers.qubit_tasks",
    "workers.sifting_tasks",
)
