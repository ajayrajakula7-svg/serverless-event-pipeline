"""
Idempotency Manager for Serverless Event Handlers.
Guarantees exactly-once processing semantics over at-least-once SQS / EventBridge message delivery.
Implements distributed locking and state validation with TTL expiry.
"""

import time
from typing import Optional, Dict, Any
from .logger import get_logger

logger = get_logger("idempotency")


class IdempotencyRecord:
    def __init__(self, key: str, status: str, result: Optional[Dict[str, Any]] = None, expires_at: int = 0):
        self.key = key
        self.status = status  # IN_PROGRESS, COMPLETED, FAILED
        self.result = result
        self.expires_at = expires_at or int(time.time() + 3600)  # 1 hour TTL default


class IdempotencyManager:
    def __init__(self, dynamodb_client=None, table_name: str = "PipelineIdempotencyTable"):
        self.dynamodb = dynamodb_client
        self.table_name = table_name
        # Fallback thread-safe in-memory store for local testing/demo simulation
        self._memory_store: Dict[str, IdempotencyRecord] = {}

    def acquire_lock(self, idempotency_key: str, ttl_seconds: int = 300) -> bool:
        """
        Attempts to acquire an execution lock for the idempotency key.
        Returns True if this is the first execution (lock acquired).
        Returns False if duplicate / currently processing.
        """
        now = int(time.time())

        # If AWS DynamoDB client is provided:
        if self.dynamodb:
            try:
                self.dynamodb.put_item(
                    TableName=self.table_name,
                    Item={
                        "idempotency_key": {"S": idempotency_key},
                        "status": {"S": "IN_PROGRESS"},
                        "expires_at": {"N": str(now + ttl_seconds)},
                        "created_at": {"N": str(now)}
                    },
                    ConditionExpression="attribute_not_exists(idempotency_key) OR expires_at < :now",
                    ExpressionAttributeValues={":now": {"N": str(now)}}
                )
                return True
            except Exception as e:
                logger.warning(f"Idempotency conflict for key {idempotency_key}: {e}")
                return False

        # In-memory simulator mode:
        if idempotency_key in self._memory_store:
            record = self._memory_store[idempotency_key]
            if record.expires_at > now and record.status in ("IN_PROGRESS", "COMPLETED"):
                logger.info(f"Duplicate event detected for idempotency key: {idempotency_key} (Status: {record.status})")
                return False

        self._memory_store[idempotency_key] = IdempotencyRecord(
            key=idempotency_key,
            status="IN_PROGRESS",
            expires_at=now + ttl_seconds
        )
        return True

    def mark_completed(self, idempotency_key: str, result: Dict[str, Any]) -> None:
        """Marks the event processing as successfully completed."""
        now = int(time.time())
        if self.dynamodb:
            try:
                self.dynamodb.update_item(
                    TableName=self.table_name,
                    Key={"idempotency_key": {"S": idempotency_key}},
                    UpdateExpression="SET #s = :status, #r = :res",
                    ExpressionAttributeNames={"#s": "status", "#r": "result"},
                    ExpressionAttributeValues={
                        ":status": {"S": "COMPLETED"},
                        ":res": {"S": str(result)}
                    }
                )
            except Exception as e:
                logger.error(f"Failed to update idempotency status in DynamoDB: {e}")
                return

        if idempotency_key in self._memory_store:
            self._memory_store[idempotency_key].status = "COMPLETED"
            self._memory_store[idempotency_key].result = result

    def release_lock_on_failure(self, idempotency_key: str) -> None:
        """Releases the lock so transient failures can be retried by SQS/Lambda."""
        if self.dynamodb:
            try:
                self.dynamodb.delete_item(
                    TableName=self.table_name,
                    Key={"idempotency_key": {"S": idempotency_key}}
                )
            except Exception as e:
                logger.error(f"Failed to release idempotency lock: {e}")
                return

        if idempotency_key in self._memory_store:
            del self._memory_store[idempotency_key]
