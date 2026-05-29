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
        #send_batch_task has NO static route  queue is set dynamically
        #by Alice at dispatch time via .set(queue=q_name).
        "workers.sifting_tasks.assemble_and_sift_task": {"queue": "sifting"},
        "workers.sifting_tasks.qber_key_task":          {"queue": "sifting"},
        "workers.sifting_tasks.notify_kme_task":        {"queue": "sifting"},
    },
    #Each worker pulls one task at a time  prevents a single worker
    #monopolising all qubit batches while others sit idle.
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    result_chord_join_timeout=300,
)

celery_app.autodiscover_tasks(["workers"])
celery_app.conf.imports = (
    "workers.qubit_tasks",
    "workers.sifting_tasks",
)