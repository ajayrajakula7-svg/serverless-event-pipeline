"""
Business Logic & Data Enrichment Service for Serverless Pipeline.
Performs domain validation, anomaly detection, scoring, and metadata augmentation.
"""

from typing import Any, Dict
from models.event_schema import EventType


class EnrichmentService:
    @staticmethod
    def enrich_event(event_type: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enriches raw payloads with analytical attributes and anomaly detection."""
        enrichment: Dict[str, Any] = {
            "processed_by_worker": "lambda-event-processor",
            "pipeline_stage": "STAGE_2_ENRICHMENT"
        }

        if event_type == EventType.TRANSACTION.value:
            amount = float(raw_data.get("amount", 0.0))
            currency = str(raw_data.get("currency", "USD")).upper()
            account_id = str(raw_data.get("account_id", "UNKNOWN"))

            # Anomaly / Fraud scoring heuristic
            fraud_score = 0.05
            flags = []

            if amount > 5000:
                fraud_score += 0.45
                flags.append("HIGH_VALUE_TRANSACTION")
            if raw_data.get("is_international", False):
                fraud_score += 0.25
                flags.append("CROSS_BORDER")
            if amount > 20000:
                fraud_score += 0.35
                flags.append("SUSPICIOUS_VELOCITY_LIMIT")

            fraud_score = min(round(fraud_score, 2), 1.0)
            risk_tier = "CRITICAL" if fraud_score >= 0.75 else ("ELEVATED" if fraud_score >= 0.4 else "LOW")

            enrichment.update({
                "risk_tier": risk_tier,
                "fraud_score": fraud_score,
                "risk_flags": flags,
                "usd_normalized_amount": round(amount * (1.0 if currency == "USD" else 1.08), 2),
                "compliance_checked": True
            })

        elif event_type == EventType.SENSOR_TELEMETRY.value:
            temp = float(raw_data.get("temperature_celsius", 25.0))
            battery = float(raw_data.get("battery_pct", 100.0))
            pressure = float(raw_data.get("pressure_kpa", 101.3))

            alerts = []
            if temp > 75.0:
                alerts.append("CRITICAL_OVERHEAT_WARNING")
            if battery < 20.0:
                alerts.append("LOW_BATTERY_SHUTDOWN_RISK")
            if pressure > 150.0:
                alerts.append("PRESSURE_OUT_OF_BOUNDS")

            health_status = "FAILING" if alerts else "NOMINAL"

            enrichment.update({
                "health_status": health_status,
                "alerts": alerts,
                "temp_fahrenheit": round((temp * 9 / 5) + 32, 2),
                "diagnostic_cycle": "OK" if not alerts else "INSPECTION_REQUIRED"
            })

        elif event_type == EventType.ORDER_CREATED.value:
            total = float(raw_data.get("total_amount", 0.0))
            items_count = int(raw_data.get("items_count", 1))

            discount_rate = 0.15 if total > 200 else (0.05 if total > 50 else 0.0)
            enrichment.update({
                "tier": "VIP_CUSTOMER" if total > 500 else "STANDARD",
                "estimated_shipping_days": 1 if total > 200 else 3,
                "applied_discount_rate": discount_rate,
                "net_payable": round(total * (1.0 - discount_rate), 2)
            })

        else:
            enrichment["generic_tag"] = "UNCLASSIFIED_TELEMETRY"

        return enrichment
