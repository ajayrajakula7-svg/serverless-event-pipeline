"""
Event Schemas and Data Models for Serverless Processing Pipeline
Supports strict validation, serialization, and schema evolution.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
import uuid
from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    TRANSACTION = "transaction.processed"
    SENSOR_TELEMETRY = "sensor.telemetry"
    ORDER_CREATED = "order.created"
    USER_ACTIVITY = "user.activity"


class ProcessingStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SENT_TO_DLQ = "SENT_TO_DLQ"


class IngestionPayload(BaseModel):
    """Raw payload model accepted by ingestion endpoint."""
    event_type: EventType
    source: str = Field(..., min_length=2, max_length=100)
    data: Dict[str, Any]
    correlation_id: Optional[str] = None
    priority: Optional[str] = Field(default="NORMAL")

    @field_validator("data")
    @classmethod
    def validate_data_not_empty(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        if not v:
            raise ValueError("Event data payload cannot be empty")
        return v


class CloudEvent(BaseModel):
    """
    Standardized CloudEvent compliant wrapper used across SQS / EventBridge.
    Ensures interoperability and uniform metadata tracing across serverless microservices.
    """
    specversion: str = "1.0"
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: str
    type: str
    time: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    datacontenttype: str = "application/json"
    correlation_id: str
    data: Dict[str, Any]
    idempotency_key: str
    retry_count: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_ingestion(cls, payload: IngestionPayload) -> "CloudEvent":
        cid = payload.correlation_id or str(uuid.uuid4())
        # Generate stable idempotency key based on source, type, and payload identity
        event_id = str(uuid.uuid4())
        return cls(
            id=event_id,
            source=payload.source,
            type=payload.event_type.value,
            correlation_id=cid,
            data=payload.data,
            idempotency_key=f"{payload.source}:{payload.event_type.value}:{cid}",
            metadata={
                "priority": payload.priority,
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }
        )


class ProcessedEventRecord(BaseModel):
    """Persistent representation stored in DynamoDB and S3 data lake."""
    pk: str = Field(..., description="Partition Key: EVENT#{id}")
    sk: str = Field(..., description="Sort Key: METADATA#{type}")
    event_id: str
    event_type: str
    source: str
    status: ProcessingStatus
    correlation_id: str
    received_at: str
    processed_at: str
    execution_duration_ms: float
    enrichment_data: Dict[str, Any] = Field(default_factory=dict)
    raw_payload: Dict[str, Any]
    error_message: Optional[str] = None
    ttl: int = Field(..., description="Unix timestamp for DynamoDB TTL automatic pruning")
