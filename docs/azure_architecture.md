# Multi-Cloud Architectural Mapping: AWS & Azure Serverless Parity

This document outlines the architectural parity between the **AWS Serverless Pipeline** and the **Azure Serverless Pipeline**. Both designs achieve zero-server maintenance, sub-second auto-scaling, decoupled buffering, and enterprise observability.

---

## 1. Architectural Component Mapping

| Functional Layer | AWS Serverless Managed Service | Azure Serverless Managed Service | Purpose & Scaling Profile |
| :--- | :--- | :--- | :--- |
| **API Ingestion** | Amazon API Gateway (HTTP API) | Azure API Management / Azure Functions HTTP Trigger | Managed endpoint with rate limiting & TLS termination |
| **Event Routing** | Amazon EventBridge / SNS | Azure Event Grid | High-throughput, low-latency publish-subscribe routing |
| **Message Buffering & Decoupling** | Amazon SQS (Standard/FIFO) with DLQ | Azure Service Bus Queue with Dead-Letter Sub-queue | At-least-once or FIFO delivery with poison pill quarantine |
| **Compute / Processing Engine** | AWS Lambda (Python 3.11+) | Azure Functions (Python v2 programming model) | Event-triggered serverless execution with auto-concurrency |
| **Operational State & Results** | Amazon DynamoDB (On-Demand billing) | Azure Cosmos DB (Serverless RU/s) | Low-latency NoSQL persistence with automatic partition management |
| **Long-Term Data Lake Archival** | Amazon S3 (Glacier lifecycle policies) | Azure Blob Storage (Cool / Archive tiering) | Compressed, immutable Parquet / JSON audit logging |
| **Monitoring & Metrics** | Amazon CloudWatch & CloudWatch EMF | Azure Monitor & Application Insights | Structured telemetry, distributed tracing, and automated alarms |

---

## 2. Azure Implementation Reference (Python v2 Model)

Below is the Azure Functions v2 implementation mirroring the AWS Lambda processor:

```python
import azure.functions as func
import json
import logging
from datetime import datetime, timezone

app = func.FunctionApp()

@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="processing-queue",
    connection="ServiceBusConnectionString"
)
@app.cosmos_db_output(
    arg_name="document",
    database_name="PipelineDB",
    container_name="EventsContainer",
    connection="CosmosDBConnectionString"
)
def process_service_bus_event(msg: func.ServiceBusMessage, document: func.Out[func.Document]) -> None:
    message_body = msg.get_body().decode("utf-8")
    logging.info(f"Processing Service Bus message ID: {msg.message_id}")
    
    event_data = json.loads(message_body)
    
    # Enrichment and business validation
    event_data["processed_at"] = datetime.now(timezone.utc).isoformat()
    event_data["id"] = msg.message_id
    event_data["status"] = "COMPLETED"
    
    # Write directly to Cosmos DB via output binding
    document.set(func.Document.from_dict(event_data))
```

---

## 3. Resilience and Scalability Comparison

### Retry & Dead Letter Queues
* **AWS**: Amazon SQS `RedrivePolicy` configures `maxReceiveCount` (default: 3). Failed messages automatically divert to a designated Dead-Letter Queue (DLQ). Lambda reports partial batch failures via `batchItemFailures`.
* **Azure**: Azure Service Bus automatically moves messages to its dead-letter sub-queue (`$DeadLetterQueue`) when `MaxDeliveryCount` is exceeded or when explicitly dead-lettered with a specific error reason.

### Concurrency & Throttling
* **AWS**: Lambda provisioned and reserved concurrency safeguards downstream databases (DynamoDB / RDS). SQS scales dynamically up to 1,000 concurrent Lambda executions.
* **Azure**: Dynamic scaling via Azure Function Consumption Plan or Flex Consumption Plan with max instance limits.
