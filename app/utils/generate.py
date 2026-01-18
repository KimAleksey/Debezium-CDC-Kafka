import time
import faker
import random
import duckdb
import logging

from app.utils.postgres import connect_to_postgres, execute_sql_query

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# Интервал с которым генерятся события. Раз в 5 сек.
interval = 5.0 / 1


def generate_record() -> dict[str, str]:
    """
    Генерируем имя фамилию и email.

    :return: Dict - словарь со значениями строки (first_name, last_name, email).
    """
    fake = faker.Faker(locale="ru_RU")
    first_name = fake.first_name().replace("'", "")
    last_name = fake.last_name().replace("'", "")
    email = fake.email()
    record = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
    }
    return record


def generate_insert_sql(alias: str | None = "pg_src") -> str:
    """
    Генерируем SQL запрос на вставку новых данных в исходный Postgres.

    :param alias: Alias Postgres.
    :return: Str - sql query.
    """
    if alias is None:
        raise Exception("Alias is required")
    record = generate_record()
    insert_sql = f"""
        INSERT INTO {alias}.inventory.customers (first_name, last_name, email)
        VALUES('{record["first_name"]}', '{record["last_name"]}', '{record["email"]}');
    """
    logging.info(f"Insert SQL: {insert_sql}")
    return insert_sql


def get_record_id(con: duckdb.DuckDBPyConnection | None, alias: str | None = "pg_src") -> int:
    """
    Получаем id записи, которую в дальнейшем будем изменять или удалять.

    :param alias: Alias Postgres.
    :param con: DB Client connection.
    :return: int - id строки.
    """
    if con is None:
        raise ValueError("Connection is required")
    if alias is None:
        raise Exception("Alias is required")
    # Получаем рандомно 1 строку.
    result = con.execute(
        f"SELECT id FROM {alias}.inventory.customers ORDER BY RANDOM() LIMIT 1"
    ).fetchall()
    # Получаем id строки, которую будем менять.
    rec_id = int(result[0][0])
    logging.info(f"Record id: {rec_id}")
    return rec_id


def generate_update_sql(rec_id: int, alias: str | None = "pg_src") -> str:
    """
    Генерируем SQL запрос на обновление данных в исходном Postgres.

    :param rec_id: int - id строки.
    :param alias: Alias Postgres.
    :return: Str - sql query.
    """
    if alias is None:
        raise Exception("Alias is required")
    if rec_id is None:
        raise Exception("Rec_id is required")
    record = generate_record()
    update_sql = f"""
        UPDATE {alias}.inventory.customers
        SET first_name='{record["first_name"]}', last_name='{record["last_name"]}', email='{record["email"]}'
        WHERE id={rec_id};
    """
    logging.info(f"Update SQL: {update_sql}")
    return update_sql


def generate_delete_sql(rec_id: int, alias: str | None = "pg_src") -> str:
    """
    Генерируем SQL запрос на удаление данных в исходном Postgres.

    :param rec_id: int - id строки.
    :param alias: Alias Postgres.
    :return: Str - sql query.
    """
    if alias is None:
        raise Exception("Alias is required")
    if rec_id is None:
        raise Exception("Rec_id is required")
    record = generate_record()
    delete_sql = f"""
        DELETE FROM {alias}.inventory.customers
        WHERE id={rec_id};
    """
    logging.info(f"Delete SQL: {delete_sql}")
    return delete_sql


def generate_event():
    alias = "pg_src"
    con = connect_to_postgres(trg=False, alias=alias)
    logging.info("Start execution.")
    last_op = 0
    try:
        while True:
            start = time.perf_counter()

            # Генерируем тип события. 1-insert, 2-update, 3-delete
            op = random.randint(1, 3)

            # Чтобы не было много удалений. Делаем подмену, если тек.событие и пред.событие = 3 удаление
            if last_op == op == 3:
                op = 1
            last_op = op

            # Выполняем событие.
            match op:
                # INSERT
                case 1:
                    # INSERT record
                    insert_sql = generate_insert_sql(alias=alias)
                    try:
                        execute_sql_query(con=con, sql_query=insert_sql)
                    except Exception as e:
                        logging.error(f"Failed to insert record. SQL query: {insert_sql}")
                # UPDATE
                case 2:
                    # GET record ID to UPDATE
                    rec_id_to_update = get_record_id(con=con, alias=alias)
                    # UPDATE record
                    update_sql = generate_update_sql(rec_id=rec_id_to_update, alias=alias)
                    try:
                        execute_sql_query(con=con, sql_query=update_sql)
                    except Exception as e:
                        logging.error(f"Failed to update record. SQL query: {update_sql}")
                case 3:
                    # GET record ID to DELETE
                    rec_id_to_delete = get_record_id(con=con, alias=alias)
                    # DELETE record
                    delete_sql = generate_delete_sql(rec_id=rec_id_to_delete, alias=alias)
                    try:
                        execute_sql_query(con=con, sql_query=delete_sql)
                    except Exception as e:
                        logging.error(f"Failed to delete record. SQL query: {delete_sql}")

            # Спим пока не пройдет секунда с момента начала.
            end = time.perf_counter() - start
            sleet_time = max(0, interval - end)
            time.sleep(sleet_time)
    except KeyboardInterrupt:
        pass
    finally:
        logging.info("Stop execution.")


if __name__ == "__main__":
    generate_event()