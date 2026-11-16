"""
Serverless Processing Lambda Worker.
Triggered asynchronously by SQS queues or EventBridge Pipes.
Implements batch consumption, idempotency guards, partial batch failure reporting,
data lake archiving, and DynamoDB persistence.
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from models.event_schema import CloudEvent, ProcessedEventRecord, ProcessingStatus
from handlers.enrichment_service import EnrichmentService
from utils.idempotency import IdempotencyManager
from utils.logger import get_logger
from utils.metrics import CloudWatchMetrics

logger = get_logger("process-lambda")


def process_single_cloud_event(
    cloud_event: CloudEvent,
    idempotency_mgr: IdempotencyManager,
    dynamodb_client=None,
    s3_client=None,
    table_name: str = "PipelineEventsTable",
    s3_bucket: str = "pipeline-data-lake-archive"
) -> ProcessedEventRecord:
    start_time = time.time()

    # 1. Check & Acquire Idempotency Lock
    if not idempotency_mgr.acquire_lock(cloud_event.idempotency_key, ttl_seconds=600):
        logger.warning(f"Skipping duplicate event {cloud_event.id} (Key: {cloud_event.idempotency_key})")
        now_iso = datetime.now(timezone.utc).isoformat()
        return ProcessedEventRecord(
            pk=f"EVENT#{cloud_event.id}",
            sk=f"METADATA#{cloud_event.type}",
            event_id=cloud_event.id,
            event_type=cloud_event.type,
            source=cloud_event.source,
            status=ProcessingStatus.COMPLETED,
            correlation_id=cloud_event.correlation_id,
            received_at=cloud_event.time,
            processed_at=now_iso,
            execution_duration_ms=0.0,
            enrichment_data={"idempotency_cached": True},
            raw_payload=cloud_event.data,
            ttl=int(time.time() + (30 * 86400))
        )

    # 2. Execute Business Logic & Anomaly Enrichment
    enrichment = EnrichmentService.enrich_event(cloud_event.type, cloud_event.data)

    # 3. Simulate processing time & compute duration
    duration_ms = round((time.time() - start_time) * 1000, 2)
    now_iso = datetime.now(timezone.utc).isoformat()

    record = ProcessedEventRecord(
        pk=f"EVENT#{cloud_event.id}",
        sk=f"METADATA#{cloud_event.type}",
        event_id=cloud_event.id,
        event_type=cloud_event.type,
        source=cloud_event.source,
        status=ProcessingStatus.COMPLETED,
        correlation_id=cloud_event.correlation_id,
        received_at=cloud_event.time,
        processed_at=now_iso,
        execution_duration_ms=duration_ms,
        enrichment_data=enrichment,
        raw_payload=cloud_event.data,
        ttl=int(time.time() + (30 * 86400))  # 30-day retention
    )

    # 4. Persist to DynamoDB (if client configured)
    if dynamodb_client:
        try:
            dynamodb_client.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": record.pk},
                    "sk": {"S": record.sk},
                    "event_id": {"S": record.event_id},
                    "event_type": {"S": record.event_type},
                    "source": {"S": record.source},
                    "status": {"S": record.status.value},
                    "correlation_id": {"S": record.correlation_id},
                    "processed_at": {"S": record.processed_at},
                    "execution_duration_ms": {"N": str(record.execution_duration_ms)},
                    "enrichment_data": {"S": json.dumps(record.enrichment_data)},
                    "ttl": {"N": str(record.ttl)}
                }
            )
        except Exception as e:
            logger.error(f"DynamoDB write error for event {record.event_id}: {e}")
            idempotency_mgr.release_lock_on_failure(cloud_event.idempotency_key)
            raise e

    # 5. Archive raw event to S3 Data Lake (partitioned by date)
    if s3_client:
        dt = datetime.now(timezone.utc)
        s3_key = f"events/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/{record.event_id}.json"
        try:
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=json.dumps(record.model_dump(), default=str),
                ContentType="application/json"
            )
        except Exception as e:
            logger.warning(f"S3 archive warning (non-fatal): {e}")

    # 6. Mark idempotency as COMPLETED
    idempotency_mgr.mark_completed(cloud_event.idempotency_key, {"status": "SUCCESS", "event_id": record.event_id})

    return record


def lambda_handler(
    event: Dict[str, Any],
    context: Any = None,
    idempotency_mgr: IdempotencyManager = None,
    dynamodb_client=None,
    s3_client=None
) -> Dict[str, Any]:
    """
    AWS Lambda entrypoint triggered by SQS.
    Implements AWS Lambda Partial Batch Failure convention:
    Returns dict with 'batchItemFailures' containing message IDs that failed.
    """
    if idempotency_mgr is None:
        idempotency_mgr = IdempotencyManager(dynamodb_client=dynamodb_client)

    metrics = CloudWatchMetrics()
    metrics.add_dimension("Handler", "ProcessLambda")

    records = event.get("Records", [])
    batch_item_failures: List[Dict[str, str]] = []
    processed_records: List[ProcessedEventRecord] = []

    logger.info(f"Processing batch of {len(records)} SQS records")

    for sqs_record in records:
        message_id = sqs_record.get("messageId", "unknown-msg-id")
        try:
            body_str = sqs_record.get("body", "{}")
            body_json = json.loads(body_str) if isinstance(body_str, str) else body_str

            # Parse CloudEvent
            cloud_event = CloudEvent(**body_json)

            # Process record
            proc_record = process_single_cloud_event(
                cloud_event=cloud_event,
                idempotency_mgr=idempotency_mgr,
                dynamodb_client=dynamodb_client,
                s3_client=s3_client
            )
            processed_records.append(proc_record)
            metrics.put_metric("EventProcessedSuccessfully", 1)

        except Exception as exc:
            logger.error(f"Critical failure processing message {message_id}: {exc}")
            # Add message ID to batchItemFailures so SQS retries only this item!
            batch_item_failures.append({"itemIdentifier": message_id})
            metrics.put_metric("EventProcessingFailures", 1)

    metrics.flush()

    return {
        "statusCode": 200,
        "batchItemFailures": batch_item_failures,
        "total_received": len(records),
        "total_processed": len(processed_records),
        "total_failed": len(batch_item_failures)
    }
