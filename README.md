# Fraud monitoring HW11

Real-time fraud scoring with Kafka, CatBoost, PostgreSQL, Prometheus and Grafana. A transaction simulator continuously publishes events, the scorer hot-reloads CatBoost models without restarting, and the writer persists enriched scores for operational and business analytics.

## Data policy

The raw dataset is not part of this repository. Its download page does not provide a redistribution license that can be safely assumed, and the rows contain person-like fields. Download `train.csv` from the authorized [TETA ML competition page](https://www.kaggle.com/competitions/teta-ml-1-2025/data) and keep it outside the repository. `*.csv`, `data/` and local `.env` files are ignored by Git.

The required columns are:

`transaction_time, merch, cat_id, amount, name_1, name_2, gender, street, one_city, us_state, post_code, lat, lon, population_city, jobs, merchant_lat, merchant_lon, target`.

## Start

Requirements: Docker Desktop or Docker Engine with the Compose plugin, at least 8 GB of free disk space, and the authorized `train.csv`.

```bash
cp .env.example .env
```

Set `TRAIN_DATA_CSV` in `.env` to the absolute path of `train.csv`, then run:

```bash
docker compose up -d --build
docker compose ps
```

The first start builds the CatBoost and Airflow images and copies the CSV into PostgreSQL. Wait until `data_loader` exits with code 0 and the long-running services are up.

| Service | Address | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | none |
| Kafka UI | http://localhost:8080 | none |
| Airflow | http://localhost:8081 | `admin` / `admin` |
| Scorer metrics | http://localhost:8000 | none |
| Writer metrics | http://localhost:8001 | none |
| PostgreSQL | `localhost:5432/fraud_db` | `fraud_user` / `fraud_pass` |

To stop without deleting volumes:

```bash
docker compose down
```

To perform a clean demonstration from empty Kafka, PostgreSQL and model volumes:

```bash
docker compose down -v
docker compose up -d --build
```

## Architecture

```text
train.csv -> transaction_simulator -> Kafka transactions
                                      |
                                      v
model registry -> CatBoost scorer -> Kafka scoring -> PostgreSQL writer -> scores
       ^                 |                                  |
       |                 +------------ metrics ------------+
       |                                                    |
Airflow retraining                                      Prometheus
                                                            |
PostgreSQL -------------------------------------------> Grafana
Node Exporter ----------------------------------------> Prometheus
```

All services share the `ml-scorer` Docker network. Prometheus scrapes every second. Grafana provisions both data sources, four dashboards and two alerts from files in the repository.

## Prometheus metrics

Scorer metrics:

- `transactions_total`: successfully scored transactions.
- `transaction_processing_seconds`: processing latency histogram.
- `fraud_score`: score histogram.
- `fraud_ratio`: cumulative share above the 0.5 threshold.
- `model_version_updates_total{version}`: model load events, including the initial version.
- `fraud_model_info{version}` and `fraud_model_reload_timestamp_seconds{version}`: active version and reload time.
- `transaction_processing_errors_total{stage}`: scoring failures.
- `transaction_last_success_timestamp_seconds`: stream liveness.
- `transaction_amount`: amount histogram. This project-specific metric helps separate latency or fraud-rate changes caused by a shift toward unusually large transactions; it is shown as Transaction amount p95 on Fraud scoring live.

Writer metrics:

- `scorings_total`: rows written.
- `scoring_db_write_seconds`: database latency histogram.
- `fraud_detected_total`: rows with `fraud_flag=1`.
- `scoring_fraud_score`: persisted score histogram.
- `scoring_db_write_errors_total`: write failures.
- `scoring_last_write_timestamp_seconds`: writer liveness.

Node Exporter provides CPU, memory, network and filesystem metrics.

## Grafana dashboards

All dashboards have the tag `fraud-monitoring-hw11`.

1. **Fraud scoring live**: TPS, p95 latency, fraud share, score density, errors, amount p95 and model-reload annotations.
2. **PostgreSQL writer live**: write TPS, p95 latency, fraud detection rate, persisted score density, write errors and time since last write.
3. **Host system metrics**: CPU, memory, network and filesystem usage.
4. **Fraud business analytics**: PostgreSQL-backed analytics with `US state` and `Merchant` filters, filtered score density and TPS, counts and fraud share by model version, fraud share by product category in the latest 1000 rows, average score by hour and a recent-score table.

If panels are empty, verify Prometheus targets at http://localhost:9090/targets and check that rows are growing:

```bash
docker compose exec -T postgres psql -U fraud_user -d fraud_db -c \
  "SELECT COUNT(*) AS scores, MAX(created_at) AS latest FROM scores;"
```

## Alerts

Grafana provisions two rules in **Alerting > Alert rules > Fraud monitoring**:

- **Fraud rate is high**: `fraud_ratio > 0.20` for 20 seconds. For a deterministic test, temporarily lower the threshold in `grafana/provisioning/alerting/rules.yaml`, restart Grafana and observe Pending then Firing.
- **Transaction stream stopped**: no successful scoring event for more than 20 seconds. Test it with `docker compose stop transaction_simulator`; restart with `docker compose start transaction_simulator`.

No email or Telegram contact point is required.

## Model retraining and hot-reload

Airflow exposes two DAGs: `supply_labeled_batch` and `retrain_catboost`. The scorer reads the shared `model_registry` volume before each inference. A retraining run writes `my_catboost_<version>.cbm` and atomically updates `latest.txt`; the next message loads that version without restarting the scorer.

To verify immediately:

1. Open Airflow and trigger `retrain_catboost` (trigger `supply_labeled_batch` first only when the next labeled month is missing).
2. Wait for the DAG to succeed.
3. Check the registry and database:

```bash
docker compose exec -T airflow sh -lc 'cat /opt/airflow/models/latest.txt && ls -1 /opt/airflow/models'
docker compose exec -T postgres psql -U fraud_user -d fraud_db -c \
  "SELECT model_version, COUNT(*), AVG(fraud_flag)::numeric(8,4) AS fraud_share FROM scores GROUP BY model_version ORDER BY model_version;"
```

4. Open Fraud scoring live. A red model annotation appears and new database rows use the new `model_version`.

## Verification commands

```bash
python3 scripts/generate_dashboards.py
python3 -m unittest discover -s tests -v
docker compose config --quiet
curl -fsS http://localhost:8000/metrics | grep '^transactions_total'
curl -fsS http://localhost:8001/metrics | grep '^scorings_total'
curl -fsS http://localhost:9090/api/v1/targets
curl -u admin:admin -fsS http://localhost:3000/api/search?tag=fraud-monitoring-hw11
curl -u admin:admin -fsS http://localhost:3000/api/v1/provisioning/alert-rules
```

The dashboard JSON is generated by `scripts/generate_dashboards.py`; edit the compact definitions and regenerate rather than hand-editing exported Grafana JSON.
