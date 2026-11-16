"""
Flask Web Dashboard & Visual Demonstration Server for Serverless Pipeline.
Provides an interactive real-time control room, event injector, and architectural topology view.
"""

import random
import uuid
from flask import Flask, jsonify, render_template_string, request
from simulator.pipeline_engine import PipelineSimulator
from models.event_schema import EventType

app = Flask(__name__)
pipeline = PipelineSimulator()

# Pre-populate with a few initial processed events so the dashboard looks vibrant immediately
def _seed_initial_events():
    for _ in range(5):
        sample_transactions = [
            {
                "event_type": EventType.TRANSACTION.value,
                "source": "payment.gateway.stripe",
                "data": {"amount": random.choice([49.99, 120.0, 5200.0, 89.50, 15000.0]), "currency": "USD", "account_id": f"acc_{random.randint(1000, 9999)}", "is_international": random.choice([True, False])}
            },
            {
                "event_type": EventType.SENSOR_TELEMETRY.value,
                "source": "iot.edge.gateway.04",
                "data": {"temperature_celsius": random.choice([24.5, 32.1, 82.4, 19.8]), "battery_pct": random.choice([95.0, 78.2, 12.0]), "pressure_kpa": 102.5}
            },
            {
                "event_type": EventType.ORDER_CREATED.value,
                "source": "ecommerce.checkout.service",
                "data": {"total_amount": random.choice([250.0, 75.0, 890.0]), "items_count": random.randint(1, 8)}
            }
        ]
        chosen = random.choice(sample_transactions)
        pipeline.ingest_event(chosen)

_seed_initial_events()

HTML_DASHBOARD = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Serverless Event-Driven Cloud Processing Pipeline | Live Control Room</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    @keyframes pulse-slow { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.7; transform: scale(1.03); } }
    .node-pulse { animation: pulse-slow 3s infinite ease-in-out; }
    .glass-card { background: rgba(17, 24, 39, 0.85); backdrop-filter: blur(12px); border: 1px solid rgba(75, 85, 99, 0.4); }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans antialiased">
  <!-- Header -->
  <header class="border-b border-slate-800 bg-slate-900/90 sticky top-0 z-50 px-6 py-4 backdrop-blur-md">
    <div class="max-w-7xl mx-auto flex flex-wrap items-center justify-between gap-4">
      <div class="flex items-center gap-3">
        <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-amber-500 via-orange-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-orange-500/20">
          <i class="fa-solid fa-bolt text-white text-lg"></i>
        </div>
        <div>
          <h1 class="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-white via-slate-200 to-slate-400">
            Serverless Event-Driven Cloud Processing Pipeline
          </h1>
          <p class="text-xs text-slate-400 flex items-center gap-2">
            <span class="inline-block w-2 h-2 rounded-full bg-emerald-500 animate-ping"></span>
            <span>AWS Lambda &bull; SQS &bull; DynamoDB &bull; S3 &bull; Azure Parity &bull; Python</span>
          </p>
        </div>
      </div>
      <div class="flex items-center gap-3">
        <span class="text-xs px-3 py-1 rounded-full bg-indigo-950/80 border border-indigo-500/40 text-indigo-300 font-mono">
          <i class="fa-solid fa-cloud mr-1"></i> Multi-Cloud Serverless Engine
        </span>
        <button onclick="refreshData()" class="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs font-medium border border-slate-700 transition">
          <i class="fa-solid fa-arrows-rotate mr-1"></i> Refresh
        </button>
      </div>
    </div>
  </header>

  <main class="max-w-7xl mx-auto px-6 py-8 space-y-8">
    <!-- Architecture Visual Topology -->
    <div class="glass-card rounded-2xl p-6 shadow-2xl relative overflow-hidden">
      <div class="flex justify-between items-center mb-6">
        <div>
          <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400 flex items-center gap-2">
            <i class="fa-solid fa-network-wired text-orange-400"></i> Active Event Pipeline Topology
          </h2>
          <p class="text-xs text-slate-400">Real-time reactive event stream flowing through decoupled managed serverless microservices</p>
        </div>
        <div class="flex items-center gap-4 text-xs font-mono">
          <span class="flex items-center gap-1.5 text-emerald-400"><i class="fa-solid fa-circle text-[8px]"></i> Healthy</span>
          <span class="flex items-center gap-1.5 text-indigo-400"><i class="fa-solid fa-shield-halved"></i> Idempotency: Active</span>
        </div>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-5 gap-4 items-center">
        <!-- Stage 1 -->
        <div class="p-4 rounded-xl bg-slate-900 border border-slate-800 text-center relative group hover:border-orange-500/50 transition">
          <div class="w-10 h-10 mx-auto rounded-lg bg-orange-500/10 text-orange-400 flex items-center justify-center mb-2">
            <i class="fa-solid fa-globe text-lg"></i>
          </div>
          <div class="text-xs font-semibold text-slate-200">API Gateway / S3</div>
          <div class="text-[10px] text-slate-400">Event Ingest Endpoint</div>
          <div class="mt-2 text-xs font-mono text-orange-400 font-bold" id="stat-ingested">0</div>
        </div>

        <!-- Arrow -->
        <div class="hidden md:flex flex-col items-center justify-center text-slate-500">
          <i class="fa-solid fa-arrow-right text-sm"></i>
          <span class="text-[9px] uppercase tracking-wider mt-1 text-slate-400">Pushed</span>
        </div>

        <!-- Stage 2 -->
        <div class="p-4 rounded-xl bg-slate-900 border border-slate-800 text-center relative group hover:border-amber-500/50 transition">
          <div class="w-10 h-10 mx-auto rounded-lg bg-amber-500/10 text-amber-400 flex items-center justify-center mb-2">
            <i class="fa-solid fa-layer-group text-lg"></i>
          </div>
          <div class="text-xs font-semibold text-slate-200">SQS Queue Buffer</div>
          <div class="text-[10px] text-slate-400">Decoupled Smoothing</div>
          <div class="mt-2 text-xs font-mono text-amber-400 font-bold">Queue Depth: <span id="stat-queue">0</span></div>
        </div>

        <!-- Arrow -->
        <div class="hidden md:flex flex-col items-center justify-center text-slate-500">
          <i class="fa-solid fa-arrow-right text-sm"></i>
          <span class="text-[9px] uppercase tracking-wider mt-1 text-slate-400">Batch Trigger</span>
        </div>

        <!-- Stage 3 -->
        <div class="p-4 rounded-xl bg-slate-900 border border-slate-800 text-center relative group hover:border-indigo-500/50 transition">
          <div class="w-10 h-10 mx-auto rounded-lg bg-indigo-500/10 text-indigo-400 flex items-center justify-center mb-2">
            <i class="fa-solid fa-microchip text-lg"></i>
          </div>
          <div class="text-xs font-semibold text-slate-200">AWS Lambda Worker</div>
          <div class="text-[10px] text-slate-400">Enrich &amp; Anomaly Eval</div>
          <div class="mt-2 text-xs font-mono text-indigo-400 font-bold" id="stat-processed">0 Processed</div>
        </div>
      </div>

      <!-- DLQ and Persistence Sub-row -->
      <div class="mt-6 pt-6 border-t border-slate-800 grid grid-cols-1 md:grid-cols-3 gap-4">
        <!-- DynamoDB -->
        <div class="flex items-center gap-3 p-3 rounded-xl bg-slate-900/60 border border-slate-800">
          <div class="w-9 h-9 rounded-lg bg-blue-500/10 text-blue-400 flex items-center justify-center text-base">
            <i class="fa-solid fa-database"></i>
          </div>
          <div>
            <div class="text-xs font-semibold text-slate-200">DynamoDB Event Store</div>
            <div class="text-[10px] text-slate-400">Single-Table NoSQL State (<span id="stat-dynamo" class="text-blue-400 font-mono font-bold">0</span> records)</div>
          </div>
        </div>

        <!-- S3 Data Lake -->
        <div class="flex items-center gap-3 p-3 rounded-xl bg-slate-900/60 border border-slate-800">
          <div class="w-9 h-9 rounded-lg bg-emerald-500/10 text-emerald-400 flex items-center justify-center text-base">
            <i class="fa-solid fa-box-archive"></i>
          </div>
          <div>
            <div class="text-xs font-semibold text-slate-200">S3 Raw Data Lake</div>
            <div class="text-[10px] text-slate-400">Long-term Parquet Archive (<span id="stat-s3" class="text-emerald-400 font-mono font-bold">0</span> objects)</div>
          </div>
        </div>

        <!-- Dead Letter Queue -->
        <div class="flex items-center justify-between p-3 rounded-xl bg-rose-950/20 border border-rose-500/30">
          <div class="flex items-center gap-3">
            <div class="w-9 h-9 rounded-lg bg-rose-500/10 text-rose-400 flex items-center justify-center text-base">
              <i class="fa-solid fa-skull-crossbones"></i>
            </div>
            <div>
              <div class="text-xs font-semibold text-rose-300">Dead-Letter Queue (DLQ)</div>
              <div class="text-[10px] text-rose-400 font-mono">Quarantined: <span id="stat-dlq" class="font-bold">0</span></div>
            </div>
          </div>
          <button onclick="triggerRedrive()" class="px-2.5 py-1 rounded bg-rose-600 hover:bg-rose-500 text-[11px] font-semibold text-white transition flex items-center gap-1 shadow-md">
            <i class="fa-solid fa-rotate-left"></i> Redrive
          </button>
        </div>
      </div>
    </div>

    <!-- Quick Metrics Bar -->
    <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
      <div class="glass-card rounded-xl p-4">
        <div class="text-xs text-slate-400 font-medium">Avg Execution Latency</div>
        <div class="text-2xl font-bold font-mono text-emerald-400 mt-1"><span id="metric-latency">0</span> <span class="text-xs text-slate-400 font-normal">ms</span></div>
        <div class="text-[10px] text-slate-400 mt-1">P95: <span id="metric-p95" class="font-mono text-slate-300">0</span> ms</div>
      </div>
      <div class="glass-card rounded-xl p-4">
        <div class="text-xs text-slate-400 font-medium">Pipeline Success Rate</div>
        <div class="text-2xl font-bold font-mono text-indigo-400 mt-1"><span id="metric-success">100</span>%</div>
        <div class="text-[10px] text-slate-400 mt-1">Zero dropped events</div>
      </div>
      <div class="glass-card rounded-xl p-4">
        <div class="text-xs text-slate-400 font-medium">Total Ingested</div>
        <div class="text-2xl font-bold font-mono text-slate-100 mt-1" id="metric-ingested">0</div>
        <div class="text-[10px] text-slate-400 mt-1">Automatic EventBridge ingress</div>
      </div>
      <div class="glass-card rounded-xl p-4">
        <div class="text-xs text-slate-400 font-medium">DLQ Redriven Count</div>
        <div class="text-2xl font-bold font-mono text-amber-400 mt-1" id="metric-redriven">0</div>
        <div class="text-[10px] text-slate-400 mt-1">Self-healing poison redrive</div>
      </div>
    </div>

    <!-- Interactive Event Injection & Live Actions -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <!-- Injection Console -->
      <div class="glass-card rounded-2xl p-6 lg:col-span-1 space-y-4">
        <div class="flex items-center justify-between">
          <h3 class="text-sm font-semibold uppercase tracking-wider text-slate-300 flex items-center gap-2">
            <i class="fa-solid fa-play text-emerald-400"></i> Event Injector
          </h3>
          <span class="text-[10px] bg-slate-800 text-slate-300 px-2 py-0.5 rounded border border-slate-700">Simulate Traffic</span>
        </div>

        <p class="text-xs text-slate-400">Trigger synthetic real-time event payloads through the serverless pipeline.</p>

        <div class="space-y-2 pt-2">
          <button onclick="injectPreset('transaction')" class="w-full py-2.5 px-3 rounded-xl bg-slate-800 hover:bg-slate-700/80 border border-slate-700 flex items-center justify-between text-xs text-slate-200 transition font-medium">
            <span class="flex items-center gap-2">
              <i class="fa-solid fa-credit-card text-emerald-400"></i>
              Financial Transaction Event
            </span>
            <i class="fa-solid fa-plus text-slate-400 text-xs"></i>
          </button>

          <button onclick="injectPreset('telemetry')" class="w-full py-2.5 px-3 rounded-xl bg-slate-800 hover:bg-slate-700/80 border border-slate-700 flex items-center justify-between text-xs text-slate-200 transition font-medium">
            <span class="flex items-center gap-2">
              <i class="fa-solid fa-satellite-dish text-cyan-400"></i>
              IoT Sensor Telemetry Event
            </span>
            <i class="fa-solid fa-plus text-slate-400 text-xs"></i>
          </button>

          <button onclick="injectPreset('order')" class="w-full py-2.5 px-3 rounded-xl bg-slate-800 hover:bg-slate-700/80 border border-slate-700 flex items-center justify-between text-xs text-slate-200 transition font-medium">
            <span class="flex items-center gap-2">
              <i class="fa-solid fa-cart-shopping text-purple-400"></i>
              E-Commerce Order Event
            </span>
            <i class="fa-solid fa-plus text-slate-400 text-xs"></i>
          </button>

          <div class="pt-4 border-t border-slate-800 space-y-2">
            <button onclick="injectBurst(25)" class="w-full py-2.5 px-3 rounded-xl bg-gradient-to-r from-indigo-600 to-indigo-700 hover:from-indigo-500 hover:to-indigo-600 text-white flex items-center justify-center gap-2 text-xs font-semibold shadow-lg shadow-indigo-600/20 transition">
              <i class="fa-solid fa-fire text-amber-300"></i>
              Burst Test (25 Concurrent Events)
            </button>

            <button onclick="injectPoisonPill()" class="w-full py-2 px-3 rounded-xl bg-rose-950/40 hover:bg-rose-900/50 border border-rose-500/40 text-rose-300 flex items-center justify-center gap-2 text-xs font-semibold transition">
              <i class="fa-solid fa-triangle-exclamation text-rose-400"></i>
              Chaos: Inject Poison Pill (DLQ Test)
            </button>
          </div>
        </div>
      </div>

      <!-- Real-time Activity Logs & CloudWatch EMF Stream -->
      <div class="glass-card rounded-2xl p-6 lg:col-span-2 flex flex-col">
        <div class="flex items-center justify-between mb-4">
          <h3 class="text-sm font-semibold uppercase tracking-wider text-slate-300 flex items-center gap-2">
            <i class="fa-solid fa-terminal text-indigo-400"></i> CloudWatch Structured Event Log Stream
          </h3>
          <span class="text-[10px] font-mono text-emerald-400 flex items-center gap-1.5">
            <span class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span> Streaming Live
          </span>
        </div>

        <div id="logs-container" class="flex-1 bg-black/60 rounded-xl p-4 font-mono text-[11px] overflow-y-auto max-h-[340px] space-y-1.5 border border-slate-800">
          <!-- Populated by JS -->
        </div>
      </div>
    </div>

    <!-- DynamoDB State & Data Lake Inspector -->
    <div class="glass-card rounded-2xl p-6">
      <div class="flex items-center justify-between mb-4">
        <div>
          <h3 class="text-sm font-semibold uppercase tracking-wider text-slate-300 flex items-center gap-2">
            <i class="fa-solid fa-table text-blue-400"></i> DynamoDB Processed Records &amp; Enrichment State
          </h3>
          <p class="text-xs text-slate-400">Inspecting single-table NoSQL partition store with enriched analytical attributes</p>
        </div>
      </div>

      <div class="overflow-x-auto">
        <table class="w-full text-left text-xs border-collapse">
          <thead>
            <tr class="border-b border-slate-800 text-slate-400 uppercase text-[10px] tracking-wider font-semibold">
              <th class="py-3 px-3">Event ID</th>
              <th class="py-3 px-3">Type</th>
              <th class="py-3 px-3">Status</th>
              <th class="py-3 px-3">Enrichment / Risk / Alert</th>
              <th class="py-3 px-3">Duration</th>
              <th class="py-3 px-3">Timestamp</th>
            </tr>
          </thead>
          <tbody id="records-body" class="divide-y divide-slate-800/60 font-mono">
            <!-- Populated by JS -->
          </tbody>
        </table>
      </div>
    </div>
  </main>

  <footer class="max-w-7xl mx-auto px-6 py-8 border-t border-slate-800 text-center text-xs text-slate-400">
    Serverless Event-Driven Cloud Processing Pipeline &bull; AWS Lambda, SQS, DynamoDB, S3, Azure Event Grid Parity
  </footer>

  <script>
    async function refreshData() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();

        // Update counts
        document.getElementById('stat-ingested').innerText = data.total_ingested;
        document.getElementById('stat-queue').innerText = data.queue_depth;
        document.getElementById('stat-processed').innerText = data.total_processed + ' Processed';
        document.getElementById('stat-dynamo').innerText = data.dynamodb_record_count;
        document.getElementById('stat-s3').innerText = data.s3_archive_count;
        document.getElementById('stat-dlq').innerText = data.dlq_depth;

        document.getElementById('metric-latency').innerText = data.avg_latency_ms;
        document.getElementById('metric-p95').innerText = data.p95_latency_ms;
        document.getElementById('metric-success').innerText = data.success_rate_pct;
        document.getElementById('metric-ingested').innerText = data.total_ingested;
        document.getElementById('metric-redriven').innerText = data.total_redriven;

        // Render logs
        const logsBox = document.getElementById('logs-container');
        logsBox.innerHTML = '';
        data.recent_logs.forEach(log => {
          const div = document.createElement('div');
          let color = 'text-slate-300';
          if (log.level === 'SUCCESS') color = 'text-emerald-400';
          if (log.level === 'WARN') color = 'text-amber-400';
          if (log.level === 'ERROR') color = 'text-rose-400';
          if (log.level === 'CRITICAL') color = 'text-red-500 font-bold';

          div.className = `flex items-start gap-2 ${color}`;
          div.innerHTML = `<span class="text-slate-400 text-[10px]">[${log.timestamp}]</span> <span class="font-bold text-[10px] w-14">[${log.level}]</span> <span>${log.message}</span>`;
          logsBox.appendChild(div);
        });

        // Render records table
        const tbody = document.getElementById('records-body');
        tbody.innerHTML = '';
        data.recent_records.reverse().forEach(rec => {
          const tr = document.createElement('tr');
          tr.className = 'hover:bg-slate-900/50 transition';

          let enrichStr = JSON.stringify(rec.enrichment_data || {});
          if (rec.enrichment_data) {
            if (rec.enrichment_data.risk_tier) {
              enrichStr = `<span class="px-2 py-0.5 rounded text-[10px] bg-slate-800 ${rec.enrichment_data.risk_tier === 'CRITICAL' ? 'text-rose-400 border border-rose-500/40' : 'text-emerald-400'}">Risk: ${rec.enrichment_data.risk_tier} (Score: ${rec.enrichment_data.fraud_score})</span>`;
            } else if (rec.enrichment_data.health_status) {
              enrichStr = `<span class="px-2 py-0.5 rounded text-[10px] bg-slate-800 ${rec.enrichment_data.health_status === 'FAILING' ? 'text-rose-400' : 'text-emerald-400'}">Health: ${rec.enrichment_data.health_status} (${rec.enrichment_data.temp_fahrenheit}°F)</span>`;
            } else if (rec.enrichment_data.tier) {
              enrichStr = `<span class="px-2 py-0.5 rounded text-[10px] bg-indigo-950 text-indigo-300 border border-indigo-500/30">${rec.enrichment_data.tier} | Discount: ${rec.enrichment_data.applied_discount_rate * 100}%</span>`;
            }
          }

          tr.innerHTML = `
            <td class="py-2.5 px-3 text-indigo-400">${rec.event_id.substring(0, 8)}...</td>
            <td class="py-2.5 px-3 text-slate-300 text-[11px]">${rec.event_type}</td>
            <td class="py-2.5 px-3"><span class="px-2 py-0.5 rounded-full text-[9px] bg-emerald-950 text-emerald-400 border border-emerald-500/30">${rec.status}</span></td>
            <td class="py-2.5 px-3">${enrichStr}</td>
            <td class="py-2.5 px-3 text-slate-400">${rec.execution_duration_ms} ms</td>
            <td class="py-2.5 px-3 text-slate-400 text-[10px]">${rec.processed_at ? rec.processed_at.split('T')[1].substring(0, 8) : ''}</td>
          `;
          tbody.appendChild(tr);
        });

      } catch (err) {
        console.error('Refresh error:', err);
      }
    }

    async function injectPreset(type) {
      let payload = {};
      if (type === 'transaction') {
        payload = {
          event_type: 'transaction.processed',
          source: 'payment.gateway.stripe',
          data: {
            amount: (Math.random() * 8000).toFixed(2),
            currency: 'USD',
            account_id: 'acc_' + Math.floor(1000 + Math.random() * 9000),
            is_international: Math.random() > 0.6
          }
        };
      } else if (type === 'telemetry') {
        payload = {
          event_type: 'sensor.telemetry',
          source: 'iot.factory.sensor.99',
          data: {
            temperature_celsius: (20 + Math.random() * 70).toFixed(1),
            battery_pct: (10 + Math.random() * 90).toFixed(1),
            pressure_kpa: 101.3
          }
        };
      } else if (type === 'order') {
        payload = {
          event_type: 'order.created',
          source: 'ecommerce.storefront',
          data: {
            total_amount: (30 + Math.random() * 450).toFixed(2),
            items_count: Math.floor(1 + Math.random() * 6)
          }
        };
      }

      await fetch('/api/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      refreshData();
    }

    async function injectBurst(count) {
      await fetch('/api/burst', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ count: count })
      });
      refreshData();
    }

    async function injectPoisonPill() {
      await fetch('/api/inject-error', { method: 'POST' });
      refreshData();
    }

    async function triggerRedrive() {
      const res = await fetch('/api/redrive-dlq', { method: 'POST' });
      const data = await res.json();
      alert(`DLQ Redrive Result:\nRe-enqueued recoverable: ${data.redriven_count}\nDiscarded poison pills: ${data.discarded_poison_pills}`);
      refreshData();
    }

    // Auto-refresh every 1.5 seconds
    setInterval(refreshData, 1500);
    window.onload = refreshData;
  </script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML_DASHBOARD)

@app.route("/api/status")
def status():
    return jsonify(pipeline.get_stats())

@app.route("/api/ingest", methods=["POST"])
def ingest():
    data = request.get_json(force=True)
    res = pipeline.ingest_event(data)
    return jsonify(res)

@app.route("/api/burst", methods=["POST"])
def burst():
    body = request.get_json(silent=True) or {}
    count = int(body.get("count", 20))
    for _ in range(count):
        etype = random.choice([EventType.TRANSACTION.value, EventType.SENSOR_TELEMETRY.value, EventType.ORDER_CREATED.value])
        pipeline.ingest_event({
            "event_type": etype,
            "source": "burst.load.generator",
            "data": {
                "amount": round(random.uniform(10, 6000), 2),
                "temperature_celsius": round(random.uniform(20, 85), 1),
                "total_amount": round(random.uniform(15, 300), 2),
                "metric_val": random.randint(1, 100)
            }
        })
    return jsonify({"status": "BURST_INGESTED", "count": count})

@app.route("/api/inject-error", methods=["POST"])
def inject_error():
    res = pipeline.ingest_event({}, inject_error=True)
    return jsonify(res)

@app.route("/api/redrive-dlq", methods=["POST"])
def redrive_dlq():
    res = pipeline.trigger_dlq_redrive()
    return jsonify(res)

if __name__ == "__main__":
    print("======================================================================")
    print("  Serverless Event-Driven Cloud Processing Pipeline - Live Dashboard")
    print("  Local Control Room: http://127.0.0.1:5000")
    print("======================================================================")
    app.run(host="0.0.0.0", port=5000, debug=False)
