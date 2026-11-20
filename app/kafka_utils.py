import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer


BOOTSTRAP_SERVERS = "localhost:9092"

# Продюсер сам повторяет отправку до DELIVERY_TIMEOUT_MS. Если и этого
# не хватило (Kafka недоступна), сообщение отправляется заново после паузы.
DELIVERY_TIMEOUT_MS = 60_000
RETRY_DELAY_SECONDS = 5


class Utf8JsonSerializer(Serializer):
    def serialize(self, topic, headers, data):
        return json.dumps(data, ensure_ascii=False).encode("utf-8")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=Utf8JsonSerializer(),
        acks="all",
        max_in_flight_requests_per_connection=1,
        delivery_timeout_ms=DELIVERY_TIMEOUT_MS,
    )


def send_with_retry(producer, topic, value):
    # Повторная отправка может дать дубликат, если первая попытка всё-таки
    # дошла до брокера. Потребители к этому готовы: обработчик отбрасывает
    # события, не соответствующие состоянию, дашборд убирает повторы.
    while True:
        try:
            return producer.send(topic, value=value).get(
                timeout=DELIVERY_TIMEOUT_MS / 1000 + 10
            )
        except KafkaError as error:
            print(
                f"Не удалось отправить в {topic} ({error}). "
                f"Повтор через {RETRY_DELAY_SECONDS} с.",
                flush=True,
            )
            time.sleep(RETRY_DELAY_SECONDS)
