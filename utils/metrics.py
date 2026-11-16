"""
CloudWatch Embedded Metric Format (EMF) emitter.
Generates high-resolution serverless metrics with zero HTTP overhead.
"""

import json
import time
from typing import Any, Dict, List, Optional
from .logger import get_logger

logger = get_logger("metrics")


class CloudWatchMetrics:
    def __init__(self, namespace: str = "Serverless/EventPipeline"):
        self.namespace = namespace
        self.metrics: Dict[str, float] = {}
        self.dimensions: Dict[str, str] = {
            "Environment": "Production",
            "Service": "EventProcessingEngine"
        }
        self.properties: Dict[str, Any] = {}

    def add_dimension(self, key: str, value: str) -> "CloudWatchMetrics":
        self.dimensions[key] = value
        return self

    def put_metric(self, name: str, value: float, unit: str = "Count") -> "CloudWatchMetrics":
        self.metrics[name] = value
        return self

    def set_property(self, key: str, value: Any) -> "CloudWatchMetrics":
        self.properties[key] = value
        return self

    def flush(self) -> Dict[str, Any]:
        """Flushes the EMF structure. In Lambda, printing EMF JSON automatically routes to CloudWatch Metrics."""
        metric_definitions: List[Dict[str, str]] = []
        for m_name in self.metrics:
            metric_definitions.append({"Name": m_name, "Unit": "Count"})

        emf_payload: Dict[str, Any] = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self.namespace,
                        "Dimensions": [list(self.dimensions.keys())],
                        "Metrics": metric_definitions
                    }
                ]
            }
        }
        emf_payload.update(self.dimensions)
        emf_payload.update(self.metrics)
        emf_payload.update(self.properties)

        return emf_payload
