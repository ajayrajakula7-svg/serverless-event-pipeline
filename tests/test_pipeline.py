"""
Unit & Integration Test Suite for Serverless Processing Pipeline.
Tests schema validation, idempotency guards, enrichment logic, partial batch failures, and DLQ handling.
"""

import json
import pytest
from pydantic import ValidationError

from models.event_schema import IngestionPayload, CloudEvent, EventType, ProcessingStatus
from handlers.enrichment_service import EnrichmentService
from handlers.ingest_handler import lambda_handler as ingest_lambda_handler
from handlers.process_handler import lambda_handler as process_lambda_handler, process_single_cloud_event
from handlers.dlq_redrive_handler import DLQRedriveService
from utils.idempotency import IdempotencyManager


def test_schema_validation_success():
    payload = {
        "event_type": EventType.TRANSACTION.value,
        "source": "payment.gateway.stripe",
        "data": {"amount": 150.0, "currency": "USD", "account_id": "acc_123"},
        "priority": "HIGH"
    }
    validated = IngestionPayload(**payload)
    cloud_event = CloudEvent.from_ingestion(validated)

    assert cloud_event.source == "payment.gateway.stripe"
    assert cloud_event.type == "transaction.processed"
    assert cloud_event.data["amount"] == 150.0
    assert cloud_event.correlation_id is not None
    assert "stripe" in cloud_event.idempotency_key


def test_schema_validation_failure():
    invalid_payload = {
        "event_type": "non.existent.type",
        "source": "",
        "data": {}
    }
    with pytest.raises(ValidationError):
        IngestionPayload(**invalid_payload)


def test_enrichment_fraud_detection():
    # High-value international transaction should be flagged as CRITICAL risk
    tx_data = {
        "amount": 25000.0,
        "currency": "EUR",
        "account_id": "acc_999",
        "is_international": True
    }
    enrichment = EnrichmentService.enrich_event(EventType.TRANSACTION.value, tx_data)

    assert enrichment["risk_tier"] == "CRITICAL"
    assert enrichment["fraud_score"] >= 0.75
    assert "HIGH_VALUE_TRANSACTION" in enrichment["risk_flags"]
    assert "CROSS_BORDER" in enrichment["risk_flags"]


def test_enrichment_iot_telemetry():
    # Overheating sensor should trigger critical alerts
    sensor_data = {
        "temperature_celsius": 92.0,
        "battery_pct": 10.0,
        "pressure_kpa": 105.0
    }
    enrichment = EnrichmentService.enrich_event(EventType.SENSOR_TELEMETRY.value, sensor_data)

    assert enrichment["health_status"] == "FAILING"
    assert "CRITICAL_OVERHEAT_WARNING" in enrichment["alerts"]
    assert "LOW_BATTERY_SHUTDOWN_RISK" in enrichment["alerts"]
    assert enrichment["temp_fahrenheit"] == 197.6


def test_idempotency_deduplication():
    mgr = IdempotencyManager()
    key = "user-order:checkout:ord_12345"

    # First attempt should acquire lock
    first_attempt = mgr.acquire_lock(key, ttl_seconds=60)
    assert first_attempt is True

    # Immediate second attempt should be recognized as duplicate and rejected
    second_attempt = mgr.acquire_lock(key, ttl_seconds=60)
    assert second_attempt is False

    # Mark completed
    mgr.mark_completed(key, {"order_id": "ord_12345"})

    # Third attempt after completion should still be rejected as duplicate
    third_attempt = mgr.acquire_lock(key, ttl_seconds=60)
    assert third_attempt is False


def test_ingest_lambda_handler_http():
    event = {
        "body": json.dumps({
            "event_type": EventType.ORDER_CREATED.value,
            "source": "shopify.store",
            "data": {"total_amount": 99.90, "items_count": 2}
        })
    }
    response = ingest_lambda_handler(event)

    assert response["statusCode"] == 202
    body = json.loads(response["body"])
    assert body["status"] == "ACCEPTED"
    assert body["pipeline_state"] == "QUEUED_FOR_PROCESSING"


def test_process_lambda_partial_batch_failure():
    mgr = IdempotencyManager()
    valid_cloud_event = CloudEvent(
        source="test.service",
        type=EventType.TRANSACTION.value,
        correlation_id="cor-1",
        data={"amount": 100.0, "account_id": "acc_1"},
        idempotency_key="tx:1"
    )

    sqs_event = {
        "Records": [
            {
                "messageId": "msg-valid-1",
                "body": valid_cloud_event.model_dump_json()
            },
            {
                "messageId": "msg-corrupted-2",
                "body": "INVALID_NOT_JSON_BODY"
            }
        ]
    }

    response = process_lambda_handler(sqs_event, idempotency_mgr=mgr)

    # Valid item processed, corrupted item reported in batchItemFailures
    assert response["statusCode"] == 200
    assert response["total_processed"] == 1
    assert response["total_failed"] == 1
    assert len(response["batchItemFailures"]) == 1
    assert response["batchItemFailures"][0]["itemIdentifier"] == "msg-corrupted-2"


def test_dlq_redrive_service():
    service = DLQRedriveService()
    dlq_messages = [
        {
            "messageId": "dlq-1",
            "body": json.dumps({
                "id": "ev-1",
                "type": EventType.TRANSACTION.value,
                "data": {"amount": 50.0},
                "retry_count": 1
            })
        },
        {
            "messageId": "dlq-2",
            "body": "MALFORMED_NON_JSON"
        }
    ]

    result = service.redrive_messages(dlq_messages, dry_run=True)

    assert result["total_analyzed"] == 2
    assert result["redriven_count"] == 1
    assert result["poison_pills"] == 1
