import json
import logging
import os
import sys
import time

import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, Summary, start_http_server

sys.path.append(os.path.abspath('./src'))  # noqa: PTH100
from preprocessing import run_preproc
from scorer import ensure_bootstrap, make_pred


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
SCORE_BUCKETS = [i / 50.0 for i in range(51)]

PROCESSING_TIME = Histogram(
    'transaction_processing_seconds',
    'Время обработки транзакции',
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)
TRANSACTION_COUNT = Counter(
    'transactions_total',
    'Общее количество обработанных транзакций',
)
FRAUD_SCORE = Histogram(
    'fraud_score',
    'Распределение скоров мошенничества',
    buckets=SCORE_BUCKETS,
)
FRAUD_RATIO = Gauge(
    'fraud_ratio',
    'Соотношение мошеннических транзакций к общему числу',
)
MODEL_VERSION_UPDATES = Counter(
    'model_version_updates_total',
    'Смена версии модели при горячей подмене',
    ['version'],
)
MODEL_INFO = Gauge(
    'fraud_model_info',
    'Активная версия модели',
    ['version'],
)
MODEL_RELOAD_TIMESTAMP = Gauge(
    'fraud_model_reload_timestamp_seconds',
    'Время загрузки версии модели',
    ['version'],
)
PROCESSING_ERRORS = Counter(
    'transaction_processing_errors_total',
    'Ошибки скоринга',
    ['stage'],
)
AMOUNT = Histogram(
    'transaction_amount',
    'Распределение сумм обработанных транзакций',
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 10000),
)
LAST_SUCCESS = Gauge(
    'transaction_last_success_timestamp_seconds',
    'Время последнего успешного скоринга',
)


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
        }
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)
        self.total_transactions = 0
        self.fraud_transactions = 0
        self._seen_model_version = None
        start_http_server(METRICS_PORT)
        logger.info("Prometheus метрики доступны на порту %s", METRICS_PORT)

    def _record_model_version(self, model_version):
        if self._seen_model_version is None:
            self._seen_model_version = model_version
            MODEL_INFO.labels(version=model_version).set(1)
            MODEL_RELOAD_TIMESTAMP.labels(version=model_version).set(time.time())
            MODEL_VERSION_UPDATES.labels(version=model_version).inc()
            return
        if model_version != self._seen_model_version:
            MODEL_INFO.labels(version=self._seen_model_version).set(0)
            MODEL_INFO.labels(version=model_version).set(1)
            MODEL_RELOAD_TIMESTAMP.labels(version=model_version).set(time.time())
            MODEL_VERSION_UPDATES.labels(version=model_version).inc()
            logger.info(
                "Модель сменилась: %s -> %s",
                self._seen_model_version,
                model_version,
            )
            self._seen_model_version = model_version

    @PROCESSING_TIME.time()
    def process_message(self, msg):
        try:
            # Десериализация JSON
            data = json.loads(msg.value().decode('utf-8'))

            # Извлекаем ID и данные
            transaction_id = data['transaction_id']
            input_df = pd.DataFrame([data['data']])

            # Препроцессинг и предсказание
            processed_df = run_preproc(input_df)
            submission, y_proba, model_version = make_pred(processed_df, "kafka_stream")

            score = float(y_proba[0])
            TRANSACTION_COUNT.inc()
            FRAUD_SCORE.observe(score)
            AMOUNT.observe(float(data['data'].get('amount') or 0))
            self.total_transactions += 1
            if score > 0.5:
                self.fraud_transactions += 1
            FRAUD_RATIO.set(self.fraud_transactions / self.total_transactions)
            self._record_model_version(model_version)

            # Добавляем ID и версию модели, которая отскорила запрос
            submission['transaction_id'] = transaction_id
            submission['model_version'] = model_version
            for column in ('us_state', 'merch', 'cat_id', 'amount'):
                submission[column] = data['data'].get(column)

            # Отправка результата в топик scoring
            self.producer.produce(
                SCORING_TOPIC,
                value=submission.to_json(orient='records'),
            )
            self.producer.flush()
            LAST_SUCCESS.set(time.time())
            return True
        except Exception as e:
            PROCESSING_ERRORS.labels(stage='message').inc()
            logger.exception(f"Error processing message: {e}")
            return False

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            
            self.process_message(msg)


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    ensure_bootstrap()
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
