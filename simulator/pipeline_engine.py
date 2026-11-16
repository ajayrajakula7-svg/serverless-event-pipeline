"""
In-Memory Serverless Pipeline Engine and Event Simulator.
Coordinates end-to-end event lifecycle: Ingestion -> SQS Queue -> Worker Concurrency ->
DynamoDB Storage -> DLQ -> Redrive with telemetry metrics.
"""

import json
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from models.event_schema import IngestionPayload, CloudEvent, EventType, ProcessingStatus, ProcessedEventRecord
from handlers.enrichment_service import EnrichmentService
from utils.idempotency import IdempotencyManager


class PipelineSimulator:
    def __init__(self):
        self.primary_queue: deque = deque()
        self.dlq: deque = deque()
        self.dynamodb_store: Dict[str, ProcessedEventRecord] = {}
        self.s3_archive_store: List[Dict[str, Any]] = []
        self.idempotency_mgr = IdempotencyManager()
        self.logs: deque = deque(maxlen=200)

        # Metrics
        self.metrics = {
            "total_ingested": 0,
            "total_processed": 0,
            "total_failed": 0,
            "total_redriven": 0,
            "latencies_ms": deque(maxlen=100),
            "start_time": time.time()
        }

        self._lock = threading.Lock()
        self.is_running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

    def log(self, level: str, message: str, meta: Optional[Dict[str, Any]] = None):
        entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            "level": level,
            "message": message,
            "meta": meta or {}
        }
        with self._lock:
            self.logs.appendleft(entry)

    def ingest_event(self, raw_payload: Dict[str, Any], inject_error: bool = False) -> Dict[str, Any]:
        """Ingests payload, validates, and places message onto SQS queue."""
        start_t = time.time()
        try:
            if inject_error:
                # Intentionally corrupted schema or poison pill
                event_id = f"poison-{int(time.time() * 1000)}"
                poison_msg = {
                    "messageId": f"msg-{event_id}",
                    "body": json.dumps({"corrupted": True, "note": "Simulated poison pill payload", "retry_count": 0}),
                    "attributes": {"ApproximateReceiveCount": 1}
                }
                with self._lock:
                    self.primary_queue.append(poison_msg)
                    self.metrics["total_ingested"] += 1
                self.log("WARN", f"Ingested simulated POISON PILL event [msg-{event_id}]", {"type": "POISON_PILL"})
                return {
                    "status": "ACCEPTED",
                    "event_id": event_id,
                    "simulated_error": True,
                    "pipeline_state": "QUEUED_FOR_PROCESSING"
                }

            # Normal path
            payload_obj = IngestionPayload(**raw_payload)
            cloud_event = CloudEvent.from_ingestion(payload_obj)

            sqs_message = {
                "messageId": f"msg-{cloud_event.id[:8]}",
                "body": cloud_event.model_dump_json(),
                "attributes": {"ApproximateReceiveCount": 1}
            }

            with self._lock:
                self.primary_queue.append(sqs_message)
                self.metrics["total_ingested"] += 1

            latency = round((time.time() - start_t) * 1000, 2)
            self.metrics["latencies_ms"].append(latency)
            self.log("INFO", f"Ingested & queued event {cloud_event.id[:8]} ({cloud_event.type})", {
                "correlation_id": cloud_event.correlation_id,
                "latency_ms": latency
            })

            return {
                "status": "ACCEPTED",
                "event_id": cloud_event.id,
                "correlation_id": cloud_event.correlation_id,
                "pipeline_state": "QUEUED_FOR_PROCESSING",
                "latency_ms": latency
            }

        except Exception as e:
            self.log("ERROR", f"Ingestion schema rejection: {str(e)}")
            return {"status": "REJECTED", "error": str(e)}

    def _worker_loop(self):
        """Simulates SQS Event Source Mapping Lambda trigger processing in micro-batches."""
        while self.is_running:
            batch = []
            with self._lock:
                # Pull batch of up to 5 items
                for _ in range(min(5, len(self.primary_queue))):
                    batch.append(self.primary_queue.popleft())

            if not batch:
                time.sleep(0.3)
                continue

            for sqs_record in batch:
                msg_id = sqs_record.get("messageId", "msg")
                body_str = sqs_record.get("body", "{}")
                receive_count = sqs_record.get("attributes", {}).get("ApproximateReceiveCount", 1)

                try:
                    body_json = json.loads(body_str)

                    # Simulate Poison Pill failure
                    if body_json.get("corrupted"):
                        raise ValueError("Critical Schema Corruption / Unparseable Payload")

                    cloud_event = CloudEvent(**body_json)

                    # Idempotency lock
                    if not self.idempotency_mgr.acquire_lock(cloud_event.idempotency_key):
                        self.log("WARN", f"Deduplicated duplicate event {cloud_event.id[:8]}")
                        continue

                    # Enrichment
                    t0 = time.time()
                    enrichment = EnrichmentService.enrich_event(cloud_event.type, cloud_event.data)
                    duration_ms = round((time.time() - t0) * 1000 + random.uniform(15, 45), 2)

                    record = ProcessedEventRecord(
                        pk=f"EVENT#{cloud_event.id}",
                        sk=f"METADATA#{cloud_event.type}",
                        event_id=cloud_event.id,
                        event_type=cloud_event.type,
                        source=cloud_event.source,
                        status=ProcessingStatus.COMPLETED,
                        correlation_id=cloud_event.correlation_id,
                        received_at=cloud_event.time,
                        processed_at=datetime.now(timezone.utc).isoformat(),
                        execution_duration_ms=duration_ms,
                        enrichment_data=enrichment,
                        raw_payload=cloud_event.data,
                        ttl=int(time.time() + 2592000)
                    )

                    with self._lock:
                        self.dynamodb_store[record.event_id] = record
                        self.s3_archive_store.append({
                            "s3_key": f"s3://data-lake/events/{record.event_id}.json",
                            "size_bytes": len(json.dumps(record.model_dump()))
                        })
                        self.metrics["total_processed"] += 1
                        self.metrics["latencies_ms"].append(duration_ms)

                    self.idempotency_mgr.mark_completed(cloud_event.idempotency_key, {"id": record.event_id})
                    self.log("SUCCESS", f"Processed event {cloud_event.id[:8]} -> DynamoDB & S3 ({duration_ms}ms)", {
                        "event_type": record.event_type,
                        "enrichment": enrichment
                    })

                except Exception as exc:
                    self.log("ERROR", f"Worker execution failure on {msg_id}: {str(exc)} (Attempt {receive_count}/3)")

                    if receive_count >= 3:
                        # Max retries exceeded -> Divert to Dead-Letter Queue
                        with self._lock:
                            self.dlq.append({
                                "messageId": msg_id,
                                "body": body_str,
                                "failure_reason": str(exc),
                                "quarantined_at": datetime.now(timezone.utc).isoformat()
                            })
                            self.metrics["total_failed"] += 1
                        self.log("CRITICAL", f"EXCEEDED RETRIES -> Diverted {msg_id} to DEAD-LETTER QUEUE (DLQ)!")
                    else:
                        # Re-enqueue for retry with exponential backoff simulation
                        sqs_record["attributes"]["ApproximateReceiveCount"] = receive_count + 1
                        with self._lock:
                            self.primary_queue.append(sqs_record)

            time.sleep(0.2)

    def trigger_dlq_redrive(self) -> Dict[str, Any]:
        """Redrives recoverable messages from DLQ back to primary queue."""
        recovered = 0
        poison_discarded = 0
        with self._lock:
            items_to_process = list(self.dlq)
            self.dlq.clear()

        for item in items_to_process:
            try:
                body = json.loads(item.get("body", "{}"))
                if body.get("corrupted"):
                    poison_discarded += 1
                    self.log("WARN", f"DLQ Sanitizer: Discarded unrecoverable poison pill {item.get('messageId')}")
                else:
                    # Recoverable message: re-enqueue
                    item["attributes"] = {"ApproximateReceiveCount": 1}
                    with self._lock:
                        self.primary_queue.append(item)
                    recovered += 1
            except Exception:
                poison_discarded += 1

        with self._lock:
            self.metrics["total_redriven"] += recovered

        self.log("INFO", f"DLQ Redrive completed: {recovered} re-enqueued, {poison_discarded} discarded poison pills")
        return {
            "redriven_count": recovered,
            "discarded_poison_pills": poison_discarded,
            "current_dlq_depth": len(self.dlq)
        }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            lats = list(self.metrics["latencies_ms"])
            avg_lat = round(sum(lats) / len(lats), 2) if lats else 0.0
            p95_lat = sorted(lats)[int(len(lats) * 0.95)] if len(lats) >= 5 else avg_lat

            total_done = self.metrics["total_processed"] + self.metrics["total_failed"]
            success_rate = round((self.metrics["total_processed"] / total_done) * 100, 1) if total_done > 0 else 100.0

            records_list = [
                r.model_dump() for r in list(self.dynamodb_store.values())[-15:]
            ]

            return {
                "queue_depth": len(self.primary_queue),
                "dlq_depth": len(self.dlq),
                "dynamodb_record_count": len(self.dynamodb_store),
                "s3_archive_count": len(self.s3_archive_store),
                "total_ingested": self.metrics["total_ingested"],
                "total_processed": self.metrics["total_processed"],
                "total_failed": self.metrics["total_failed"],
                "total_redriven": self.metrics["total_redriven"],
                "avg_latency_ms": avg_lat,
                "p95_latency_ms": p95_lat,
                "success_rate_pct": success_rate,
                "recent_records": records_list,
                "recent_logs": list(self.logs)[:25]
            }
