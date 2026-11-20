import json
import time
from decimal import Decimal, ROUND_HALF_UP

from kafka import KafkaConsumer, TopicPartition

from app.kafka_utils import BOOTSTRAP_SERVERS, create_producer, send_with_retry


EVENTS_TOPIC = "taxi.events"
COEFFICIENTS_TOPIC = "taxi.coefficients"
PRICES_TOPIC = "taxi.prices"
ALERTS_TOPIC = "taxi.alerts"

LOAD_WEIGHT = 0.5
QUEUE_WEIGHT = 1.0
MAX_MULTIPLIER = 3.0

BASE_FARE = Decimal("3.00")
PRICE_PER_KM = Decimal("0.80")
PRICE_PER_MINUTE = Decimal("0.15")  
MIN_FARE = Decimal("5.00")

ALERT_THRESHOLD = 1.5


def calculate_multiplier(total_drivers, free_drivers, waiting_orders):
    if total_drivers == 0:
        return None

    utilization = (total_drivers - free_drivers) / total_drivers

    queue_ratio = waiting_orders / total_drivers

    multiplier = 1 + LOAD_WEIGHT * utilization + QUEUE_WEIGHT * queue_ratio

    return min(MAX_MULTIPLIER, multiplier)


def calculate_price(distance_km, duration_min, multiplier):
    if multiplier is None:
        return None

    base_price = (
        BASE_FARE
        + Decimal(str(distance_km)) * PRICE_PER_KM
        + Decimal(str(duration_min)) * PRICE_PER_MINUTE
    )

    price = max(MIN_FARE, base_price * Decimal(str(multiplier)))

    return float(price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def get_status(total_drivers, free_drivers, waiting_orders, multiplier):
    if total_drivers == 0:
        return "NO_DATA"

    if free_drivers == 0 and waiting_orders > 0:
        return "NO_DRIVERS"

    if multiplier >= ALERT_THRESHOLD:
        return "HIGH_DEMAND"

    return "NORMAL"


def publish(producer, topic, result):
    send_with_retry(producer, topic, result)

    print(f'{topic}: {json.dumps(result, ensure_ascii=False)}', flush=True)


def apply_event(event, all_drivers, free_drivers, waiting_orders, active_trips):
    event_type = event["event_type"]

    if event_type == "simulation_started":
        # Состояние уже сброшено при смене run_id в main().
        return

    if event_type == "driver_available":
        driver_id = event["driver_id"]

        if driver_id in all_drivers:
            raise ValueError(
                f"Повторная регистрация водителя: {driver_id}"
            )

        all_drivers.add(driver_id)
        free_drivers.add(driver_id)

    elif event_type == "order_created":
        order_id = event["order_id"]
        distance = event["distance_km"]
        duration = event["duration_min"]

        if distance <= 0 or duration <= 0:
            raise ValueError(
                "Расстояние и длительность должны быть > 0"
            )

        if (
            order_id in waiting_orders
            or order_id in active_trips.values()
        ):
            raise ValueError(
                f"Заказ уже существует: {order_id}"
            )

        waiting_orders.add(order_id)

    elif event_type == "order_assigned":
        order_id = event["order_id"]
        driver_id = event["driver_id"]

        if order_id not in waiting_orders:
            raise ValueError(
                f"Заказ не ожидает назначения: {order_id}"
            )

        if driver_id not in free_drivers:
            raise ValueError(
                f"Водитель не свободен: {driver_id}"
            )

        waiting_orders.remove(order_id)
        free_drivers.remove(driver_id)
        active_trips[driver_id] = order_id

    elif event_type == "trip_completed":
        driver_id = event["driver_id"]
        order_id = event["order_id"]

        if active_trips.get(driver_id) != order_id:
            raise ValueError(
                "Завершение не соответствует активной поездке"
            )

        del active_trips[driver_id]
        free_drivers.add(driver_id)

    elif event_type == "order_cancelled":
        order_id = event["order_id"]

        if order_id not in waiting_orders:
            raise ValueError(
                f"Заказ не находится в очереди: {order_id}"
            )

        waiting_orders.remove(order_id)

    else:
        raise ValueError(
            f"Неизвестный тип события: {event_type}"
        )


def new_state():
    return {
        "all_drivers": set(),
        "free_drivers": set(),
        "waiting_orders": set(),
        # driver_id → order_id
        "active_trips": {},
        "previous_status": "NO_DATA",
    }


def read_last_published_offset(events_end_offset):
    """Offset последнего входного события, по которому уже есть результат.

    Обработчик при старте заново читает taxi.events, чтобы восстановить
    состояние, но публикует результаты только для событий после этого
    offset. Так перезапуск не дублирует выходные топики.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        enable_auto_commit=False,
    )

    try:
        partition = TopicPartition(COEFFICIENTS_TOPIC, 0)
        consumer.assign([partition])

        end = consumer.end_offsets([partition])[partition]

        if end == 0:
            return -1

        consumer.seek(partition, end - 1)

        deadline = time.monotonic() + 10

        while time.monotonic() < deadline:
            for messages in consumer.poll(timeout_ms=1000).values():
                for message in messages:
                    try:
                        offset = json.loads(message.value)["source_offset"]
                    except (ValueError, KeyError, TypeError):
                        # Результат старой версии без source_offset.
                        return -1

                    # Входной топик пересоздан, а выходной нет.
                    if offset >= events_end_offset:
                        return -1

                    return offset

        raise RuntimeError(
            f"Не удалось прочитать последнее сообщение {COEFFICIENTS_TOPIC}"
        )

    finally:
        consumer.close()


def check_run(event, current_run_id, finished_runs):
    """Возвращает True, если событие начинает новую симуляцию."""
    run_id = event.get("run_id")

    if run_id is None:
        raise ValueError("нет run_id: событие старой версии генератора")

    if run_id == current_run_id:
        return False

    if run_id in finished_runs:
        raise ValueError(f"событие завершённой симуляции {run_id}")

    return True


def main():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        enable_auto_commit=False,
    )

    partition = TopicPartition(EVENTS_TOPIC, 0)
    consumer.assign([partition])
    consumer.seek_to_beginning(partition)

    events_end_offset = consumer.end_offsets([partition])[partition]
    last_published = read_last_published_offset(events_end_offset)

    producer = create_producer()

    state = new_state()
    current_run_id = None
    finished_runs = set()

    replay_skipped = 0
    replay_announced = last_published < 0

    print(
        "Обработчик запущен. Входной топик читается с начала; "
        f"результаты уже опубликованы до offset {last_published}.",
        flush=True,
    )

    try:
        for message in consumer:
            # События до last_published уже обработаны прежним запуском:
            # восстанавливаем по ним состояние, но не публикуем повторно.
            replaying = message.offset <= last_published

            if not replaying and not replay_announced:
                print(
                    f"Состояние восстановлено по {message.offset} событиям "
                    f"(пропущено некорректных: {replay_skipped}).",
                    flush=True,
                )
                replay_announced = True

            try:
                event = json.loads(message.value.decode("utf-8"))

                if not isinstance(event, dict):
                    raise ValueError("Событие должно быть объектом")

                event_type = event["event_type"]
                source_event_id = event["event_id"]
                simulation_time = event["simulation_time_min"]

                if check_run(event, current_run_id, finished_runs):
                    if current_run_id is not None:
                        finished_runs.add(current_run_id)

                    current_run_id = event["run_id"]
                    state = new_state()

                    if not replaying:
                        print(
                            f"Новая симуляция: run_id={current_run_id}",
                            flush=True,
                        )

                apply_event(
                    event,
                    state["all_drivers"],
                    state["free_drivers"],
                    state["waiting_orders"],
                    state["active_trips"],
                )

            except (
                ValueError, KeyError, TypeError, UnicodeError, AttributeError
            ) as error:
                if replaying:
                    replay_skipped += 1
                else:
                    print(
                        f'Пропуск сообщения offset={message.offset}: {error}',
                        flush=True,
                    )
                continue

            total_count = len(state["all_drivers"])
            free_count = len(state["free_drivers"])
            waiting_count = len(state["waiting_orders"])

            multiplier = calculate_multiplier(total_count, free_count, waiting_count)

            utilization = (
                (total_count - free_count) / total_count
                if total_count > 0
                else None
            )

            status = get_status(total_count, free_count, waiting_count, multiplier)
            previous_status = state["previous_status"]
            state["previous_status"] = status

            if replaying:
                continue

            result = {
                "source_event_id": source_event_id,
                "source_offset": message.offset,
                "run_id": current_run_id,
                "simulation_time_min": simulation_time,
                "total_drivers": total_count,
                "free_drivers": free_count,
                "busy_drivers": len(state["active_trips"]),
                "waiting_orders": waiting_count,
                "utilization": (
                    round(utilization, 4)
                    if utilization is not None
                    else None
                ),
                "multiplier": (
                    round(multiplier, 4)
                    if multiplier is not None
                    else None
                ),
                "status": status,
                "service_available": free_count > 0,
            }

            if (
                status != previous_status
                and status in {"HIGH_DEMAND", "NO_DRIVERS"}
            ):
                publish(
                    producer,
                    ALERTS_TOPIC,
                    {
                        **result,
                        "previous_status": previous_status,
                    },
                )

            if event_type == "order_created":
                price = calculate_price(
                    event["distance_km"],
                    event["duration_min"],
                    multiplier,
                )

                publish(
                    producer,
                    PRICES_TOPIC,
                    {
                        **result,
                        "order_id": event["order_id"],
                        "distance_km": event["distance_km"],
                        "duration_min": event["duration_min"],
                        "estimated_price": price,
                        "currency": "BYN",
                    },
                )

            # Коэффициенты публикуются последними: по их source_offset
            # следующий запуск определяет, какие события уже обработаны
            # полностью.
            publish(producer, COEFFICIENTS_TOPIC, result)

    except KeyboardInterrupt:
        print("\nОбработчик остановлен.", flush=True)

    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
