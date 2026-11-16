"""
Dead-Letter Queue (DLQ) Redrive & Poison Message Handler.
Analyzes failed events from SQS DLQ, isolates poison pills, and replays recoverable messages.
"""

import json
import time
from typing import Any, Dict, List
from utils.logger import get_logger
from utils.metrics import CloudWatchMetrics

logger = get_logger("dlq-redrive")


class DLQRedriveService:
    def __init__(self, primary_queue_url: str = None, dlq_url: str = None, sqs_client=None):
        self.primary_queue_url = primary_queue_url
        self.dlq_url = dlq_url
        self.sqs = sqs_client

    def redrive_messages(self, messages: List[Dict[str, Any]], dry_run: bool = False) -> Dict[str, Any]:
        metrics = CloudWatchMetrics()
        metrics.add_dimension("Handler", "DLQRedrive")

        redriven_count = 0
        poison_pill_count = 0
        details = []

        for msg in messages:
            msg_id = msg.get("messageId", "dlq-msg")
            body_str = msg.get("body", "{}")

            try:
                payload = json.loads(body_str) if isinstance(body_str, str) else body_str

                # Determine if recoverable
                # If event lacks critical correlation or schema is irreparably corrupted, mark as poison
                if not isinstance(payload, dict) or "type" not in payload:
                    poison_pill_count += 1
                    details.append({
                        "message_id": msg_id,
                        "status": "DISCARDED_POISON_PILL",
                        "reason": "Missing required CloudEvent schema envelope"
                    })
                    continue

                # Add retry backoff metadata
                current_retries = payload.get("retry_count", 0) + 1
                payload["retry_count"] = current_retries
                payload["metadata"] = payload.get("metadata", {})
                payload["metadata"]["redriven_from_dlq_at"] = int(time.time())

                if not dry_run and self.sqs and self.primary_queue_url:
                    self.sqs.send_message(
                        QueueUrl=self.primary_queue_url,
                        MessageBody=json.dumps(payload),
                        MessageAttributes={
                            "RedriveAttempt": {"DataType": "Number", "StringValue": str(current_retries)}
                        }
                    )

                redriven_count += 1
                details.append({
                    "message_id": msg_id,
                    "event_id": payload.get("id"),
                    "status": "REDRIVEN_TO_PRIMARY_QUEUE",
                    "attempt": current_retries
                })

            except json.JSONDecodeError as err:
                logger.warning(f"Malformed JSON in DLQ message {msg_id}: {err}")
                poison_pill_count += 1
                details.append({
                    "message_id": msg_id,
                    "status": "DISCARDED_POISON_PILL",
                    "reason": f"Non-JSON payload: {err}"
                })
            except Exception as err:
                logger.error(f"Failed to process DLQ message {msg_id}: {err}")
                poison_pill_count += 1
                details.append({
                    "message_id": msg_id,
                    "status": "ERROR",
                    "reason": str(err)
                })

        metrics.put_metric("DLQMessagesRedriven", redriven_count)
        metrics.put_metric("DLQPoisonPillsIdentified", poison_pill_count)
        metrics.flush()

        return {
            "total_analyzed": len(messages),
            "redriven_count": redriven_count,
            "poison_pills": poison_pill_count,
            "dry_run": dry_run,
            "results": details
        }
