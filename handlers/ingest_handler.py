"""
Serverless Ingestion Lambda Handler.
Entrypoint for API Gateway, S3 ObjectCreated triggers, or EventBridge partners.
Validates schemas, assigns correlation/trace IDs, and enqueues to SQS.
"""

import json
import os
import time
from typing import Any, Dict
from pydantic import ValidationError

from models.event_schema import IngestionPayload, CloudEvent
from utils.logger import get_logger
from utils.metrics import CloudWatchMetrics

logger = get_logger("ingest-lambda")


def lambda_handler(event: Dict[str, Any], context: Any = None, sqs_client=None, queue_url: str = None) -> Dict[str, Any]:
    """
    AWS Lambda entrypoint.
    event: API Gateway Proxy event format or direct dictionary.
    """
    start_time = time.time()
    metrics = CloudWatchMetrics()
    metrics.add_dimension("Handler", "IngestLambda")

    # Extract body if invoked through API Gateway
    if "body" in event:
        raw_body = event["body"]
        if isinstance(raw_body, str):
            try:
                body = json.loads(raw_body)
            except json.JSONDecodeError as err:
                logger.error(f"Invalid JSON payload: {err}")
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": "Malformed JSON payload", "details": str(err)})
                }
        else:
            body = raw_body
    else:
        body = event

    # Schema Validation
    try:
        validated_input = IngestionPayload(**body)
    except ValidationError as err:
        logger.warning(f"Schema validation failed: {err.errors()}")
        metrics.put_metric("SchemaValidationErrors", 1).flush()
        return {
            "statusCode": 422,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "error": "Unprocessable Entity",
                "message": "Payload failed strict schema validation",
                "validation_errors": err.errors()
            })
        }

    # Transform to CloudEvent specification
    cloud_event = CloudEvent.from_ingestion(validated_input)

    # Push to SQS Queue
    target_queue_url = queue_url or os.getenv("PROCESSING_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123456789012/pipeline-processing-queue")

    if sqs_client:
        try:
            sqs_response = sqs_client.send_message(
                QueueUrl=target_queue_url,
                MessageBody=cloud_event.model_dump_json(),
                MessageAttributes={
                    "EventType": {
                        "DataType": "String",
                        "StringValue": cloud_event.type
                    },
                    "CorrelationId": {
                        "DataType": "String",
                        "StringValue": cloud_event.correlation_id
                    }
                }
            )
            message_id = sqs_response.get("MessageId", "sqs-msg-id")
        except Exception as exc:
            logger.error(f"Failed to push message to SQS: {exc}")
            metrics.put_metric("QueuePublishErrors", 1).flush()
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Failed to enqueue event", "details": str(exc)})
            }
    else:
        # Local / Simulation fallback ID
        message_id = f"sim-{cloud_event.id[:8]}"

    elapsed_ms = round((time.time() - start_time) * 1000, 2)
    metrics.put_metric("IngestionSuccess", 1)
    metrics.put_metric("IngestionLatencyMs", elapsed_ms)
    metrics.flush()

    logger.info(
        f"Event successfully ingested and queued: {cloud_event.id}",
        extra={"extra_fields": {"event_id": cloud_event.id, "type": cloud_event.type, "latency_ms": elapsed_ms}}
    )

    return {
        "statusCode": 202,
        "headers": {
            "Content-Type": "application/json",
            "X-Correlation-Id": cloud_event.correlation_id
        },
        "body": json.dumps({
            "status": "ACCEPTED",
            "event_id": cloud_event.id,
            "correlation_id": cloud_event.correlation_id,
            "message_id": message_id,
            "ingestion_latency_ms": elapsed_ms,
            "pipeline_state": "QUEUED_FOR_PROCESSING"
        })
    }
