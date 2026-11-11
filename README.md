# Мониторинг мошеннических транзакций — ДЗ 5

Система потокового скоринга транзакций на основе Kafka, CatBoost, PostgreSQL, Prometheus и Grafana. Симулятор непрерывно отправляет транзакции в Kafka, сервис скоринга применяет модель CatBoost и подхватывает новые версии без перезапуска, а writer сохраняет обогащённые результаты в PostgreSQL для технического и бизнес-мониторинга.

## Используемые данные

Исходный датасет не включён в репозиторий. На странице загрузки нет лицензии, которая явно разрешала бы повторное распространение, а строки содержат поля, похожие на персональные данные. Загрузите `train.csv` из доступного вам источника — [страницы соревнования TETA ML](https://www.kaggle.com/competitions/teta-ml-1-2025/data) — и храните файл вне репозитория. Файлы `*.csv`, каталог `data/` и локальный `.env` исключены через `.gitignore`.

Ожидаемые столбцы:

`transaction_time, merch, cat_id, amount, name_1, name_2, gender, street, one_city, us_state, post_code, lat, lon, population_city, jobs, merchant_lat, merchant_lon, target`.

## Быстрый запуск

Понадобятся:

- Docker Desktop или Docker Engine с плагином Compose;
- не менее 8 ГБ свободного места;
- файл `train.csv`, полученный законным способом.

Создайте локальный файл настроек:

```bash
cp .env.example .env
```

Укажите в `.env` абсолютный путь к `train.csv` в переменной `TRAIN_DATA_CSV`, затем запустите стек:

```bash
docker compose up -d --build
docker compose ps
```

При первом запуске собираются образы CatBoost и Airflow, а данные загружаются в PostgreSQL. Дождитесь, пока одноразовый сервис `data_loader` завершится с кодом `0`, а постоянные сервисы перейдут в состояние `Up`.

| Сервис | Адрес | Данные для входа |
|---|---|---|
| Grafana | <http://localhost:3000> | `admin` / `admin` |
| Prometheus | <http://localhost:9090> | не требуются |
| Kafka UI | <http://localhost:8080> | не требуются |
| Airflow | <http://localhost:8081> | `admin` / `admin` |
| Метрики scorer | <http://localhost:8000> | не требуются |
| Метрики writer | <http://localhost:8001> | не требуются |
| PostgreSQL | `localhost:5432/fraud_db` | `fraud_user` / `fraud_pass` |

Остановка без удаления данных:

```bash
docker compose down
```

Чистый демонстрационный запуск с пустыми томами Kafka, PostgreSQL и реестра моделей:

```bash
docker compose down -v
docker compose up -d --build
```

## Архитектура

```text
train.csv -> симулятор транзакций -> Kafka: transactions
                                           |
                                           v
реестр моделей -> CatBoost scorer -> Kafka: scoring -> PostgreSQL writer -> scores
       ^                  |                                      |
       |                  +-------------- метрики ---------------+
       |                                                         |
переобучение Airflow                                         Prometheus
                                                                 |
PostgreSQL -------------------------------------------------> Grafana
Node Exporter ---------------------------------------------> Prometheus
```

Все контейнеры работают в Docker-сети `ml-scorer`. Prometheus опрашивает сервисы каждую секунду. Grafana автоматически получает два источника данных, четыре дашборда и два правила оповещения из файлов репозитория.

## Метрики Prometheus

Метрики scorer:

- `transactions_total` — количество успешно обработанных транзакций;
- `transaction_processing_seconds` — гистограмма времени обработки;
- `fraud_score` — распределение fraud score;
- `fraud_ratio` — накопленная доля транзакций со score выше `0.5`;
- `model_version_updates_total{version}` — загрузки версий модели, включая начальную;
- `fraud_model_info{version}` — активная версия модели;
- `fraud_model_reload_timestamp_seconds{version}` — время загрузки версии;
- `transaction_processing_errors_total{stage}` — ошибки скоринга по этапам;
- `transaction_last_success_timestamp_seconds` — время последнего успешного скоринга;
- `transaction_amount` — распределение сумм транзакций. На дашборде отображается p95, чтобы отделять изменения нагрузки и fraud rate от сдвига в сторону необычно крупных покупок.

Метрики writer:

- `scorings_total` — количество строк, записанных в PostgreSQL;
- `scoring_db_write_seconds` — гистограмма задержки записи;
- `fraud_detected_total` — количество строк с `fraud_flag=1`;
- `scoring_fraud_score` — распределение сохранённых значений score;
- `scoring_db_write_errors_total` — ошибки записи;
- `scoring_last_write_timestamp_seconds` — время последней успешной записи.

Node Exporter предоставляет метрики процессора, памяти, сети и файловой системы.

## Дашборды Grafana

Все дашборды помечены техническим тегом `fraud-monitoring-hw11`:

1. **Fraud scoring live** — TPS, p95 задержки, доля мошенничества, распределение score, ошибки, p95 суммы транзакции и аннотации смены модели.
2. **PostgreSQL writer live** — TPS записи, p95 задержки БД, частота обнаружения мошенничества, распределение сохранённых score, ошибки и время с последней записи.
3. **Host system metrics** — процессор, память, сеть и файловая система.
4. **Fraud business analytics** — аналитика PostgreSQL с фильтрами `US state` и `Merchant`, отфильтрованными score density и TPS, показателями по версиям модели, долей мошенничества по товарным категориям для последних 1000 строк, средним score по часам и таблицей последних результатов.

Если панели пустые, проверьте цели Prometheus на <http://localhost:9090/targets> и рост таблицы `scores`:

```bash
docker compose exec -T postgres psql -U fraud_user -d fraud_db -c \
  "SELECT COUNT(*) AS scores, MAX(created_at) AS latest FROM scores;"
```

## Алерты Grafana

В разделе **Alerting → Alert rules → Fraud monitoring** автоматически создаются два правила:

- **Fraud rate is high** — `fraud_ratio > 0.20` в течение 20 секунд. Для детерминированной демонстрации можно временно снизить порог в `grafana/provisioning/alerting/rules.yaml`, перезапустить Grafana и дождаться состояний `Pending`, затем `Firing`.
- **Transaction stream stopped** — успешный скоринг отсутствует более 20 секунд. Для проверки остановите поток командой `docker compose stop transaction_simulator`, затем восстановите его командой `docker compose start transaction_simulator`.

Подключение email или Telegram для выполнения задания не требуется.

## Переобучение и горячая смена модели

Airflow содержит два DAG:

- `supply_labeled_batch` добавляет очередной размеченный временной пакет;
- `retrain_catboost` формирует обучающую выборку, обучает CatBoost и публикует новую версию.

Scorer проверяет общий том `model_registry` перед каждым предсказанием. После переобучения появляется файл `my_catboost_<version>.cbm`, а `latest.txt` атомарно переключается на новую версию. Следующее сообщение обрабатывается новой моделью без перезапуска scorer.

Проверка hot-reload:

1. Откройте Airflow и запустите `retrain_catboost`. `supply_labeled_batch` требуется запускать вручную только при отсутствии следующего размеченного месяца.
2. Дождитесь состояния `success` у DAG.
3. Проверьте реестр и версии в PostgreSQL:

```bash
docker compose exec -T airflow sh -lc 'cat /opt/airflow/models/latest.txt && ls -1 /opt/airflow/models'
docker compose exec -T postgres psql -U fraud_user -d fraud_db -c \
  "SELECT model_version, COUNT(*), AVG(fraud_flag)::numeric(8,4) AS fraud_share FROM scores GROUP BY model_version ORDER BY model_version;"
```

4. Откройте **Fraud scoring live**. На графике появится красная аннотация смены модели, а новые строки PostgreSQL будут содержать новый `model_version`.

## Проверка проекта

```bash
python3 scripts/generate_dashboards.py
python3 -m unittest discover -s tests -v
docker compose config --quiet
curl -fsS http://localhost:8000/metrics | grep '^transactions_total'
curl -fsS http://localhost:8001/metrics | grep '^scorings_total'
curl -fsS http://localhost:9090/api/v1/targets
curl -u admin:admin -fsS 'http://localhost:3000/api/search?tag=fraud-monitoring-hw11'
curl -u admin:admin -fsS http://localhost:3000/api/v1/provisioning/alert-rules
```

JSON-файлы дашбордов создаются скриптом `scripts/generate_dashboards.py`. Изменяйте компактные определения в скрипте и запускайте генерацию повторно вместо ручного редактирования экспортированного JSON.

## Автоматическая проверка GitHub

Workflow **Build Docker Images** запускается при каждом push и pull request в `main`. Он без внешних секретов собирает Docker-образы `fraud_detector`, `transaction_simulator`, `scoring_writer`, `data_loader` и `airflow`. Успешный workflow подтверждает, что все пять образов воспроизводимо собираются в чистой среде GitHub Actions.
