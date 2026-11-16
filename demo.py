"""
CLI Demonstration Script for Serverless Event-Driven Cloud Processing Pipeline.
Run this script to immediately demonstrate:
1. Event Ingestion & Schema Validation
2. Decoupled SQS Message Buffering
3. Lambda Auto-Scaling & Concurrency Processing
4. Idempotent Deduplication Verification
5. Anomaly Detection & Business Enrichment
6. Persistence to DynamoDB & S3 Data Lake Archive
7. Chaos Error Injection & Dead-Letter Queue (DLQ) Quarantine
8. DLQ Redrive & Automated Recovery
"""

import sys
import time

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from colorama import Fore, Style, init
from simulator.pipeline_engine import PipelineSimulator
from models.event_schema import EventType

init(autoreset=True)


def print_banner():
    print(f"\n{Fore.CYAN}==========================================================================================")
    print(f"{Fore.YELLOW}{Style.BRIGHT}  SERVERLESS EVENT-DRIVEN CLOUD PROCESSING PIPELINE")
    print(f"{Fore.WHITE}  AWS Lambda | Amazon SQS | DynamoDB | S3 Data Lake | Azure Event Grid Parity | Python")
    print(f"{Fore.CYAN}=========================================================================================={Style.RESET_ALL}\n")


def run_demo():
    print_banner()
    pipeline = PipelineSimulator()
    time.sleep(0.5)

    print(f"{Fore.BLUE}[STEP 1/6]{Fore.WHITE} Ingesting valid heterogeneous event streams (Transactions, IoT, Orders)...")
    events = [
        {
            "event_type": EventType.TRANSACTION.value,
            "source": "stripe.webhook.prod",
            "data": {"amount": 7450.00, "currency": "USD", "account_id": "acc_9921", "is_international": True}
        },
        {
            "event_type": EventType.SENSOR_TELEMETRY.value,
            "source": "iot.turbofan.sensor.12",
            "data": {"temperature_celsius": 88.5, "battery_pct": 14.2, "pressure_kpa": 160.0}
        },
        {
            "event_type": EventType.ORDER_CREATED.value,
            "source": "shopify.storefront.api",
            "data": {"total_amount": 349.00, "items_count": 4}
        }
    ]

    for ev in events:
        res = pipeline.ingest_event(ev)
        print(f"  {Fore.GREEN}[OK] Ingested:{Fore.WHITE} {ev['event_type']} -> ID: {Fore.YELLOW}{res['event_id'][:8]}...{Fore.WHITE} (Queued in SQS)")
        time.sleep(0.15)

    print(f"\n{Fore.BLUE}[STEP 2/6]{Fore.WHITE} Testing Idempotency & Duplicate Event Guard...")
    dup_res = pipeline.ingest_event(events[0])
    print(f"  {Fore.MAGENTA}[*] Duplicate payload sent:{Fore.WHITE} {events[0]['event_type']} (Correlation: {dup_res.get('correlation_id', 'same')[:8]})")
    print(f"  {Fore.GREEN}[OK] Idempotency Manager:{Fore.WHITE} Acquired lock & verified idempotency cache.")

    print(f"\n{Fore.BLUE}[STEP 3/6]{Fore.WHITE} Simulating High-Throughput Burst Load (15 concurrent events)...")
    for i in range(15):
        pipeline.ingest_event({
            "event_type": EventType.TRANSACTION.value,
            "source": f"mobile.app.client.{i}",
            "data": {"amount": round(50.0 + (i * 35.5), 2), "currency": "USD", "account_id": f"acc_{2000+i}"}
        })
    print(f"  {Fore.GREEN}[OK] Burst enqueued into SQS.{Fore.WHITE} Lambda worker pool consuming in micro-batches...")

    print(f"\n{Fore.BLUE}[STEP 4/6]{Fore.WHITE} Waiting for asynchronous Lambda workers to process queue...")
    for _ in range(8):
        stats = pipeline.get_stats()
        print(f"  {Fore.CYAN}--> Active SQS Queue Depth: {stats['queue_depth']} | DynamoDB Persisted: {stats['dynamodb_record_count']} | S3 Lake: {stats['s3_archive_count']}{Fore.WHITE}")
        if stats["queue_depth"] == 0:
            break
        time.sleep(0.6)

    print(f"\n{Fore.BLUE}[STEP 5/6]{Fore.WHITE} Injecting Poison Pill to Demonstrate Dead-Letter Queue (DLQ) Quarantine...")
    poison_res = pipeline.ingest_event({}, inject_error=True)
    print(f"  {Fore.RED}[!] Injected Corrupted Event:{Fore.WHITE} {poison_res['event_id']}")
    print(f"  {Fore.YELLOW}--> Lambda retries with exponential backoff (Attempt 1, 2, 3)...")
    time.sleep(1.5)

    stats = pipeline.get_stats()
    print(f"  {Fore.RED}[X] Max retries reached.{Fore.WHITE} Poison pill safely quarantined into {Fore.RED}Dead-Letter Queue (DLQ Depth: {stats['dlq_depth']}){Fore.WHITE}")

    print(f"\n{Fore.BLUE}[STEP 6/6]{Fore.WHITE} Triggering Automated DLQ Redrive Workflow...")
    redrive_res = pipeline.trigger_dlq_redrive()
    print(f"  {Fore.GREEN}[OK] DLQ Redrive Completed:{Fore.WHITE} Discarded poisoned schema: {redrive_res['discarded_poison_pills']}, Recovered: {redrive_res['redriven_count']}")

    time.sleep(0.5)
    final_stats = pipeline.get_stats()

    print(f"\n{Fore.GREEN}{Style.BRIGHT}==========================================================================================")
    print(f"{Fore.GREEN}{Style.BRIGHT}  PIPELINE EXECUTION SUMMARY")
    print(f"{Fore.GREEN}{Style.BRIGHT}==========================================================================================")
    print(f"  Total Ingested Events : {Fore.YELLOW}{final_stats['total_ingested']}")
    print(f"  Total Processed Events: {Fore.GREEN}{final_stats['total_processed']}")
    print(f"  DynamoDB Items Stored : {Fore.CYAN}{final_stats['dynamodb_record_count']}")
    print(f"  S3 Data Lake Objects  : {Fore.CYAN}{final_stats['s3_archive_count']}")
    print(f"  Avg Lambda Latency    : {Fore.MAGENTA}{final_stats['avg_latency_ms']} ms (P95: {final_stats['p95_latency_ms']} ms)")
    print(f"  Pipeline Success Rate : {Fore.GREEN}{final_stats['success_rate_pct']}%")
    print(f"{Fore.GREEN}{Style.BRIGHT}=========================================================================================={Style.RESET_ALL}\n")
    print(f"{Fore.YELLOW}Tip: To launch the full interactive web dashboard, run: {Fore.WHITE}python app.py{Style.RESET_ALL}\n")


if __name__ == "__main__":
    run_demo()
