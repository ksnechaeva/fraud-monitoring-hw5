#!/usr/bin/env python3
"""Generate project-owned Grafana dashboards from compact, reviewable definitions."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "grafana" / "dashboards"
PROM = {"type": "prometheus", "uid": "PBFA97CFB590B2093"}
POSTGRES = {"type": "postgres", "uid": "fraud-postgres"}


def grid(index, width=12, height=8):
    return {"h": height, "w": width, "x": (index % 2) * 12, "y": (index // 2) * height}


def prom_panel(panel_id, title, expr, index, *, unit="short", panel_type="timeseries", legend=""):
    return {
        "id": panel_id, "title": title, "type": panel_type, "gridPos": grid(index),
        "datasource": PROM,
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
        "targets": [{"refId": "A", "expr": expr, "legendFormat": legend}],
    }


def sql_panel(panel_id, title, raw_sql, index, *, fmt="time_series", panel_type="timeseries", unit="short"):
    return {
        "id": panel_id, "title": title, "type": panel_type, "gridPos": grid(index),
        "datasource": POSTGRES,
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
        "targets": [{"refId": "A", "format": fmt, "rawQuery": True, "rawSql": raw_sql}],
    }


def dashboard(title, uid, panels, *, variables=None, annotations=None):
    return {
        "id": None, "uid": uid, "title": title, "tags": ["fraud-monitoring-hw11"],
        "timezone": "browser", "schemaVersion": 39, "version": 1,
        "refresh": "5s", "time": {"from": "now-15m", "to": "now"},
        "templating": {"list": variables or []}, "panels": panels,
        "annotations": {"list": annotations or []},
    }


scoring = dashboard(
    "Fraud scoring live",
    "fraud-scoring-live",
    [
        prom_panel(1, "Processing TPS", "rate(transactions_total[30s])", 0, unit="ops"),
        prom_panel(2, "Processing latency p95", "histogram_quantile(0.95, sum by (le) (rate(transaction_processing_seconds_bucket[2m])))", 1, unit="s"),
        prom_panel(3, "Current fraud share", "fraud_ratio", 2, unit="percentunit"),
        prom_panel(4, "Score density", "sum by (le) (rate(fraud_score_bucket[2m]))", 3, legend="score <= {{le}}"),
        prom_panel(5, "Scoring errors", "rate(transaction_processing_errors_total[2m])", 4, unit="ops", legend="{{stage}}"),
        prom_panel(6, "Transaction amount p95", "histogram_quantile(0.95, sum by (le) (rate(transaction_amount_bucket[2m])))", 5, unit="currencyUSD"),
    ],
    annotations=[{
        "name": "Model reload", "enable": True, "datasource": PROM, "iconColor": "red",
        "expr": "increase(model_version_updates_total[30s]) > 0",
        "titleFormat": "Model {{version}} loaded", "tagKeys": "version",
    }],
)

writer = dashboard(
    "PostgreSQL writer live",
    "postgres-writer-live",
    [
        prom_panel(1, "Writes per second", "rate(scorings_total[30s])", 0, unit="ops"),
        prom_panel(2, "DB write latency p95", "histogram_quantile(0.95, sum by (le) (rate(scoring_db_write_seconds_bucket[2m])))", 1, unit="s"),
        prom_panel(3, "Fraud detections per second", "rate(fraud_detected_total[30s])", 2, unit="ops"),
        prom_panel(4, "Written score density", "sum by (le) (rate(scoring_fraud_score_bucket[2m]))", 3, legend="score <= {{le}}"),
        prom_panel(5, "Write errors", "rate(scoring_db_write_errors_total[2m])", 4, unit="ops"),
        prom_panel(6, "Seconds since last write", "time() - scoring_last_write_timestamp_seconds", 5, unit="s"),
    ],
)

system = dashboard(
    "Host system metrics",
    "host-system-metrics",
    [
        prom_panel(1, "CPU busy", "1 - avg(rate(node_cpu_seconds_total{mode=\"idle\"}[2m]))", 0, unit="percentunit"),
        prom_panel(2, "Memory used", "1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes", 1, unit="percentunit"),
        prom_panel(3, "Network throughput", "sum by (device) (rate(node_network_receive_bytes_total[2m]) + rate(node_network_transmit_bytes_total[2m]))", 2, unit="Bps", legend="{{device}}"),
        prom_panel(4, "Filesystem used", "1 - node_filesystem_avail_bytes{fstype!~\"tmpfs|overlay\"} / node_filesystem_size_bytes{fstype!~\"tmpfs|overlay\"}", 3, unit="percentunit", legend="{{mountpoint}}"),
    ],
)

variables = [
    {"name": "state", "label": "US state", "type": "query", "datasource": POSTGRES,
     "query": "SELECT DISTINCT us_state AS __text, us_state AS __value FROM scores WHERE us_state IS NOT NULL ORDER BY 1",
     "refresh": 2, "includeAll": True, "allValue": "%", "multi": False},
    {"name": "merch", "label": "Merchant", "type": "query", "datasource": POSTGRES,
     "query": "SELECT DISTINCT merch AS __text, merch AS __value FROM scores WHERE merch IS NOT NULL AND us_state LIKE '$state' ORDER BY 1",
     "refresh": 2, "includeAll": True, "allValue": "%", "multi": False},
]
filtered = "us_state LIKE '$state' AND merch LIKE '$merch'"
business = dashboard(
    "Fraud business analytics",
    "fraud-business-analytics",
    [
        sql_panel(1, "Score density by selected state and merchant",
                  f"SELECT width_bucket(score, 0, 1, 20) / 20.0 AS bucket, COUNT(*) AS count FROM scores WHERE $__timeFilter(created_at) AND {filtered} GROUP BY 1 ORDER BY 1",
                  0, fmt="table", panel_type="barchart"),
        sql_panel(2, "Filtered processing TPS",
                  f"SELECT date_trunc('second', created_at) AS time, COUNT(*)::double precision AS tps FROM scores WHERE $__timeFilter(created_at) AND {filtered} GROUP BY 1 ORDER BY 1",
                  1, unit="ops"),
        sql_panel(3, "Fraud share by model version",
                  "SELECT model_version, COUNT(*) AS scores, AVG(fraud_flag)::double precision AS fraud_share FROM scores WHERE $__timeFilter(created_at) GROUP BY model_version ORDER BY model_version",
                  2, fmt="table", panel_type="table", unit="percentunit"),
        sql_panel(4, "Fraud share by category in last 1000 transactions",
                  "WITH recent AS (SELECT cat_id, fraud_flag FROM scores ORDER BY created_at DESC LIMIT 1000) SELECT cat_id, AVG(fraud_flag)::double precision AS fraud_share FROM recent GROUP BY cat_id ORDER BY fraud_share DESC",
                  3, fmt="table", panel_type="barchart", unit="percentunit"),
        sql_panel(5, "Average score by transaction hour",
                  "SELECT EXTRACT(HOUR FROM created_at)::int AS hour, AVG(score)::double precision AS avg_score FROM scores WHERE $__timeFilter(created_at) GROUP BY 1 ORDER BY 1",
                  4, fmt="table", panel_type="barchart", unit="percentunit"),
        sql_panel(6, "Latest scores",
                  f"SELECT created_at, us_state, merch, cat_id, amount, score, fraud_flag, model_version FROM scores WHERE {filtered} ORDER BY created_at DESC LIMIT 100",
                  5, fmt="table", panel_type="table"),
    ],
    variables=variables,
)

OUT.mkdir(parents=True, exist_ok=True)
for old in OUT.glob("*.json"):
    old.unlink()
for name, content in (
    ("fraud-scoring-live.json", scoring),
    ("postgres-writer-live.json", writer),
    ("host-system-metrics.json", system),
    ("fraud-business-analytics.json", business),
):
    (OUT / name).write_text(json.dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

