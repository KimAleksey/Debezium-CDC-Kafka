import json
import logging

from confluent_kafka import Consumer, KafkaError, TopicPartition

from app.utils.utils import get_secret
from app.utils.postgres import connect_to_postgres, manipulate_record

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def consume_message(topic: str | None = None, offset: int | None = None) -> None:
    """
    Функция считывает сообщения из topic с заданным offset и выводит считанное сообщение в консоль.

    :param topic: Имя топика.
    :param offset: Оффсет.
    :return: None.
    """
    # Kafka server
    bootstrap_server = get_secret("KAFKA_BOOTSTRAP_SERVER")

    # Конфигурация Consumer
    conf = {
        "bootstrap.servers": bootstrap_server,
        "group.id": "consumer.py",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    # Создаем Consumer
    consumer = Consumer(conf)

    if not topic:
        raise ValueError("Необходимо передать параметр topic.")

    if offset is not None:
        # Получаем список партиций topic.
        partitions = consumer.list_topics(topic).topics[topic].partitions
        for partition in partitions:
            # Ручное назначение топика, партиции и offset.
            consumer.assign([TopicPartition(topic, partition, offset)])
    else:
        # Обычная подписка на topic.
        consumer.subscribe([topic])

    try:
        # Подключаемся к целевому Postgres
        con = connect_to_postgres(trg=True, alias="pg_trg")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError:
                    logging.error("Reached end of partition.")
                else:
                    logging.error(f"Error: {msg.error()}")
            else:
                try:
                    raw = msg.value()
                    if raw is None:
                        logging.info("Получено сообщение с пустым Value. Пропускаем это сообщение.")
                        continue
                    decoded_msg = raw.decode("utf-8")
                    obj = json.loads(decoded_msg)
                    payload = obj["payload"]
                    logging.info(f"Received message with payload: {payload}")
                    try:
                        manipulate_record(con=con, payload=payload, trg=True, alias="pg_trg")
                        consumer.commit(msg)
                    except Exception as e:
                        logging.error(f"Can't process message with payload: {payload}. Error: {e}")
                except Exception as e:
                    logging.error(f"Error: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    topic_name = get_secret("KAFKA_USERS_COORDINATES_TOPIC")

    # Читаем с начала
    consume_message(topic=topic_name)

    # Читаем с определенного offset.
    # consume_message(topic=topic_name, offset=0)