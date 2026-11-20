import json
import random
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.kafka_utils import create_producer, send_with_retry


DRIVERS_COUNT = 50
BASE_ORDERS_PER_MINUTE = 0.4

SIMULATION_SPEED = 60
TICK_SECONDS = 0.2

MIN_PATIENCE_MIN = 3
MAX_PATIENCE_MIN = 12

CONFIG_PATH = (Path(__file__).resolve().parent.parent / "simulation.json")


def read_activity(previous):
    try:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        value = config["client_activity"]

        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not 0 <= value < float("inf")
        ):
            raise ValueError("client_activity должна быть конечным числом >= 0")

        return float(value)

    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Настройки не прочитаны: {error}", flush=True)
        return previous


def next_arrival(now, activity):
    if activity == 0:
        return float("inf")

    rate = BASE_ORDERS_PER_MINUTE * activity
    return now + random.expovariate(rate)


def send_event(producer, now, event_type, **fields):
    event = {
        "event_id": uuid4().hex,
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "simulation_time_min": round(now, 3),
        **fields,
    }

    send_with_retry(producer, 'taxi.events', event)

    print(json.dumps(event, ensure_ascii=False), flush=True)


def create_order(created_at):
    distance = round(random.uniform(2, 12), 1)
    speed = random.uniform(20, 40)
    duration = round(distance / speed * 60, 1)
    patience = random.uniform(MIN_PATIENCE_MIN, MAX_PATIENCE_MIN)

    return {
        "order_id": uuid4().hex,
        "distance_km": distance,
        "duration_min": duration,
        "created_at": created_at,
        "cancel_at": created_at + patience,
    }


def finish_trips(producer, drivers, now):
    for driver_id, trip in drivers.items():
        if trip is not None and now >= trip["available_at"]:
            send_event(
                producer,
                now,
                "trip_completed",
                driver_id=driver_id,
                order_id=trip["order_id"],
            )

            drivers[driver_id] = None


def add_orders(producer, waiting_orders, now, next_order_at, activity):
    while now >= next_order_at:
        order = create_order(next_order_at)

        send_event(
            producer,
            now,
            "order_created",
            order_id=order["order_id"],
            distance_km=order["distance_km"],
            duration_min=order["duration_min"],
            created_at_min=round(order["created_at"], 3),
        )

        waiting_orders.append(order)

        next_order_at = next_arrival(next_order_at, activity)

    return next_order_at


def cancel_orders(producer, waiting_orders, now):
    remaining_orders = deque()

    for order in waiting_orders:
        if now >= order["cancel_at"]:
            send_event(
                producer,
                now,
                "order_cancelled",
                order_id=order["order_id"],
                reason="waiting_timeout",
                waited_min=round(
                    now - order["created_at"], 2
                ),
            )
        else:
            remaining_orders.append(order)

    return remaining_orders


def assign_orders(producer, drivers, waiting_orders, now):
    for driver_id, trip in drivers.items():
        if not waiting_orders:
            break

        if trip is not None:
            continue

        order = waiting_orders[0]
        pickup_min = round(random.uniform(1, 4), 1)

        available_at = now + pickup_min + order['duration_min']

        send_event(
            producer,
            now,
            "order_assigned",
            order_id=order["order_id"],
            driver_id=driver_id,
            distance_km=order["distance_km"],
            duration_min=order["duration_min"],
            pickup_min=pickup_min,
            waited_min=round(
                now - order["created_at"], 2
            ),
        )

        waiting_orders.popleft()

        drivers[driver_id] = {
            "order_id": order["order_id"],
            "available_at": available_at,
        }


def main():
    producer = create_producer()

    # None означает, что водитель свободен.
    drivers = {f'driver-{number:03d}': None for number in range(1, DRIVERS_COUNT + 1)}

    waiting_orders = deque()

    try:
        for driver_id in drivers:
            send_event(producer, 0.0, 'driver_available', driver_id=driver_id)

        activity = read_activity(1.0)
        next_order_at = next_arrival(0.0, activity)

        started_at = time.monotonic()
        next_config_check = started_at

        print(
            f"\nГенератор запущен. Активность: {activity}. "
            "Остановка: Ctrl+C.\n",
            flush=True,
        )

        while True:
            real_now = time.monotonic()
            now = (real_now - started_at) * SIMULATION_SPEED / 60

            if real_now >= next_config_check:
                new_activity = read_activity(activity)

                if new_activity != activity:
                    activity = new_activity
                    next_order_at = next_arrival(now, activity)

                    print(f'\nНовая активность: {activity}\n', flush=True)

                next_config_check = real_now + 1.0

            finish_trips(producer, drivers, now)

            next_order_at = add_orders(
                producer, waiting_orders, now, next_order_at, activity
            )

            waiting_orders = cancel_orders(producer, waiting_orders, now)

            assign_orders(producer, drivers, waiting_orders, now)

            time.sleep(TICK_SECONDS)

    except KeyboardInterrupt:
        print("\nГенератор остановлен.", flush=True)

    finally:
        producer.close()


if __name__ == "__main__":
    main()
