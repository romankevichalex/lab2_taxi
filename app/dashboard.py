import json
from bisect import bisect_right

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer, TopicPartition


BOOTSTRAP_SERVERS = "localhost:9092"

STATE_TOPIC = "taxi.coefficients"
PRICE_TOPIC = "taxi.prices"

HISTORY_MINUTES = 120
AVERAGE_WINDOW_MINUTES = 10

st.set_page_config(page_title='Мониторинг такси', layout='wide')

st.title("Мониторинг службы такси")

st.caption(
    "Время на графиках — модельные минуты. "
    "Обновление страницы — раз в реальную секунду."
)


def initialize():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        enable_auto_commit=False,
    )

    partitions = [TopicPartition(STATE_TOPIC, 0), TopicPartition(PRICE_TOPIC, 0)]

    consumer.assign(partitions)
    consumer.seek_to_beginning(*partitions)

    st.session_state["consumer"] = consumer
    st.session_state["states"] = []
    st.session_state["prices"] = []

    st.session_state['seen'] = {STATE_TOPIC: set(), PRICE_TOPIC: set()}


def read_messages():
    consumer = st.session_state["consumer"]

    for _ in range(5):
        batches = consumer.poll(timeout_ms=100, max_records=1000)

        if not batches:
            break

        for partition, messages in batches.items():
            topic = partition.topic
            seen = st.session_state["seen"][topic]

            for message in messages:
                event = json.loads(message.value.decode("utf-8"))
                event_id = event["source_event_id"]

                if event_id in seen:
                    continue

                seen.add(event_id)

                if topic == STATE_TOPIC:
                    st.session_state["states"].append(event)

                elif topic == PRICE_TOPIC:
                    st.session_state["prices"].append(event)


def prepare_states():
    states = st.session_state["states"]

    if not states:
        return pd.DataFrame()

    frame = pd.DataFrame(states)

    frame = (
        frame.sort_values(
            "simulation_time_min",
            kind="stable",
        )
        .drop_duplicates(
            subset="simulation_time_min",
            keep="last",
        )
    )

    return frame.reset_index(drop=True)


def calculate_average_prices(times):
    prices = st.session_state["prices"]

    values = sorted(
        (
            float(event["simulation_time_min"]),
            float(event["estimated_price"]),
        )
        for event in prices
        if event.get("estimated_price") is not None
    )

    price_times = [time for time, _ in values]

    # Накопленные суммы для вычисления среднего в каждом окне.
    sums = [0.0]

    for _, price in values:
        sums.append(sums[-1] + price)

    averages = []

    for current_time in times:
        left = bisect_right(price_times, current_time - AVERAGE_WINDOW_MINUTES)
        right = bisect_right(price_times, current_time)

        count = right - left

        average = (sums[right] - sums[left]) / count if count > 0 else float('nan')

        averages.append(average)

    return averages


def trim_history(latest_time):
    # Дополнительный запас нужен для среднего в начале графика.
    cutoff = latest_time - HISTORY_MINUTES - AVERAGE_WINDOW_MINUTES

    for key in ("states", "prices"):
        st.session_state[key] = [
            event
            for event in st.session_state[key]
            if event["simulation_time_min"] >= cutoff
        ]


@st.fragment(run_every="1s")
def dashboard():
    try:
        read_messages()
    except Exception as error:
        st.error(f"Ошибка чтения данных: {error}")
        st.caption(
            "Kafka "
        )
        return

    states = prepare_states()

    if states.empty:
        st.info('Нет данных.')
        return

    latest = states.iloc[-1]
    latest_time = float(latest["simulation_time_min"])

    chart = states[
        states["simulation_time_min"]
        >= latest_time - HISTORY_MINUTES
    ].copy()

    chart["average_price"] = calculate_average_prices(
        chart["simulation_time_min"].tolist()
    )

    last_average = chart["average_price"].iloc[-1]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Свободные водители",
        f"{int(latest['free_drivers'])} / "
        f"{int(latest['total_drivers'])}",
    )

    col2.metric('Ожидающие заказы', int(latest['waiting_orders']))

    multiplier = latest["multiplier"]

    col3.metric(
        "Коэффициент",
        f"{multiplier:.2f}"
        if pd.notna(multiplier)
        else "Нет данных",
    )

    col4.metric(
        f"Средняя цена за {AVERAGE_WINDOW_MINUTES} мин",
        f"{last_average:.2f} BYN"
        if pd.notna(last_average)
        else "Нет заказов",
    )

    st.caption(f"Модельное время: {latest_time:.1f} мин. Статус: {latest['status']}.")

    graph = chart.rename(
        columns={
            "simulation_time_min": "Модельное время, мин",
            "free_drivers": "Свободные",
            "busy_drivers": "Занятые",
            "waiting_orders": "Ожидающие заказы",
            "multiplier": "Коэффициент",
            "average_price": "Средняя цена, BYN",
        }
    ).set_index("Модельное время, мин")

    left, right = st.columns(2)

    with left:
        st.subheader("Водители")
        st.line_chart(graph[['Свободные', 'Занятые']], height=280)

        st.subheader("Коэффициент спроса")
        st.line_chart(graph[['Коэффициент']], height=280)

    with right:
        st.subheader("Очередь заказов")
        st.line_chart(graph[['Ожидающие заказы']], height=280)

        st.subheader("Средняя предварительная цена")
        st.line_chart(graph[['Средняя цена, BYN']], height=280)

    st.caption(
        "Средняя цена рассчитана по новым заказам за последние "
        f"{AVERAGE_WINDOW_MINUTES} модельных минут. "
        "Если заказ позднее отменён, его предварительная оценка "
        "остаётся в статистике."
    )

    trim_history(latest_time)


if "consumer" not in st.session_state:
    try:
        initialize()
    except Exception as error:
        st.error(f"Не удалось подключиться к Kafka: {error}")
        st.stop()

dashboard()
