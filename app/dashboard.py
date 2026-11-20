import json
import threading
import time
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


class Feed:
    """Один фоновый читатель Kafka на весь сервер Streamlit.

    Все вкладки браузера показывают одно и то же состояние, поэтому
    отдельный потребитель на каждую сессию не нужен: он перечитывал бы
    топики с начала при каждом обновлении страницы и никогда не закрывался.

    Хранятся только сообщения текущей симуляции (run_id) за последние
    HISTORY_MINUTES + AVERAGE_WINDOW_MINUTES модельных минут.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._run_id = None
        self._finished_runs = set()
        self._states = []
        self._prices = []
        self._error = None

        threading.Thread(
            target=self._run, name="kafka-feed", daemon=True
        ).start()

    def snapshot(self):
        with self._lock:
            return (
                self._run_id,
                list(self._states),
                list(self._prices),
                self._error,
            )

    def _set_error(self, error):
        with self._lock:
            self._error = error

    def _connect(self):
        while True:
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    group_id=None,
                    enable_auto_commit=False,
                )

                partitions = [
                    TopicPartition(STATE_TOPIC, 0),
                    TopicPartition(PRICE_TOPIC, 0),
                ]

                consumer.assign(partitions)
                consumer.seek_to_beginning(*partitions)

                return consumer

            except Exception as error:
                self._set_error(f"Не удалось подключиться к Kafka: {error}")
                time.sleep(5)

    def _run(self):
        consumer = self._connect()

        while True:
            try:
                batches = consumer.poll(timeout_ms=500, max_records=1000)
            except Exception as error:
                self._set_error(f"Ошибка чтения данных: {error}")
                time.sleep(5)
                continue

            with self._lock:
                self._error = None

                for partition, messages in batches.items():
                    for message in messages:
                        self._add(partition.topic, message)

                self._trim()

    def _add(self, topic, message):
        try:
            event = json.loads(message.value.decode("utf-8"))
            run_id = event["run_id"]
            event["simulation_time_min"] = float(event["simulation_time_min"])
        except (ValueError, KeyError, TypeError, UnicodeError, AttributeError):
            # Повреждённое сообщение или результат старой версии без run_id.
            return

        if run_id != self._run_id:
            # Сообщения уже сменившейся симуляции не показываются.
            if run_id in self._finished_runs:
                return

            if self._run_id is not None:
                self._finished_runs.add(self._run_id)

            self._run_id = run_id
            self._states = []
            self._prices = []

        if topic == STATE_TOPIC:
            self._states.append(event)
        elif topic == PRICE_TOPIC:
            self._prices.append(event)

    def _trim(self):
        if not self._states:
            return

        latest_time = max(
            event["simulation_time_min"] for event in self._states
        )

        # Дополнительный запас нужен для среднего в начале графика.
        cutoff = latest_time - HISTORY_MINUTES - AVERAGE_WINDOW_MINUTES

        self._states = [
            event for event in self._states
            if event["simulation_time_min"] >= cutoff
        ]
        self._prices = [
            event for event in self._prices
            if event["simulation_time_min"] >= cutoff
        ]


@st.cache_resource
def get_feed():
    return Feed()


def prepare_states(states):
    if not states:
        return pd.DataFrame()

    # Повторы возможны после повторной отправки результата обработчиком.
    frame = pd.DataFrame(states).drop_duplicates(
        subset="source_event_id", keep="last"
    )

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


def calculate_average_prices(prices, times):
    unique_prices = {
        event["source_event_id"]: event
        for event in prices
        if event.get("estimated_price") is not None
    }

    values = sorted(
        (
            float(event["simulation_time_min"]),
            float(event["estimated_price"]),
        )
        for event in unique_prices.values()
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


@st.fragment(run_every="1s")
def dashboard():
    run_id, states, prices, error = get_feed().snapshot()

    if error:
        st.warning(f"{error}. Повторная попытка через несколько секунд.")

    states = prepare_states(states)

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
        prices, chart["simulation_time_min"].tolist()
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

    st.caption(
        f"Модельное время: {latest_time:.1f} мин. "
        f"Статус: {latest['status']}. Симуляция: {run_id[:8]}."
    )

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


dashboard()
