import duckdb
import logging

from app.utils.utils import get_secret

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def connect_to_postgres(trg: bool | None = True, alias: str | None = "pg_trg") -> duckdb.DuckDBPyConnection:
    """
    Подключение к DB Postgres. По-умолчанию подключается к целевому Postgres.

    :param trg: Флаг подключения к целевому Postgres. Иначе - к источнику.
    :param alias: Alias Postgres.
    :return: Postgres connection.
    """
    if trg is None:
        raise ValueError("Target parameter is required")
    if alias is None:
        raise ValueError("Alias parameter is required")
    if trg:
        trg_or_src = "_TRG"
        logging.info("Подключение к целевому Postgres DB.")
    else:
        trg_or_src = "_SRC"
        logging.info("Подключение к исходному Postgres DB.")

    # Подключаем DuckDB
    con = duckdb.connect()

    # Установка нужных коннекторов
    con.execute("""
        INSTALL postgres; 
        LOAD postgres;
    """)

    # Устанавливаем параметры соединения
    conn_params = {
        "host": get_secret("POSTGRES_HOST" + trg_or_src, True),
        "port": get_secret("POSTGRES_PORT" + trg_or_src, True),
        "user": get_secret("POSTGRES_USER" + trg_or_src, False),
        "password": get_secret("POSTGRES_PASSWORD" + trg_or_src, False),
        "database": get_secret("POSTGRES_DB" + trg_or_src, True),
    }

    # Подключение к Postgres
    con.execute(f"""
    ATTACH 
        'host={conn_params["host"]} 
         port={conn_params["port"]} 
         user={conn_params["user"]} 
         password={conn_params["password"]} 
         dbname={conn_params["database"]}' 
    AS {alias} (TYPE postgres);
    """)
    logging.info("Подключение к Postgres успешно.")

    return con


def execute_sql_query(con: duckdb.DuckDBPyConnection | None, sql_query: str | None) -> None:
    """
    Выполнение SQL запроса в DB.

    :param sql_query: Исполняемый запрос.
    :param con: DB Client connection.
    :return: None.
    """
    if con is None:
        raise ValueError("Connection parameter is required")
    if sql_query is None:
        raise ValueError("SQL query parameter is required")
    con.execute(sql_query)


def init_postgres_trg(alias: str | None = "pg_trg") -> None:
    """
    Выполняет первичную настройку целевой DB. Создаем схему и таблицу.

    :param alias: Alias Postgres.
    :return: None.
    """
    if alias is None:
        raise ValueError("Alias parameter is required")

    con = connect_to_postgres(trg=True, alias=alias)
    create_inventory_schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {alias}.inventory;
    """
    try:
        con.execute(create_inventory_schema_sql)
        logging.info("Схема inventory создана.")
    except Exception as e:
        raise RuntimeError(e)

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {alias}.inventory.customers (
            id integer NOT NULL,
            first_name varchar(255) NOT NULL,
            last_name varchar(255) NOT NULL,
            email varchar(255) NOT NULL
        );
    """
    try:
        con.execute(create_table_sql)
        logging.info("Таблица inventory.customers создана.")
    except Exception as e:
        raise RuntimeError(e)


def insert_record_to_pg(
        con: duckdb.DuckDBPyConnection | None,
        after: dict[str, str] | None,
        alias: str | None = "pg_trg"
) -> None:
    """
    INSERT данных в таблицу inventory.customers.

    :param con: DB Client connection.
    :param after: Словарь с данными для вставки, полученный из Debezium.
    :param alias: Alias Postgres.
    :return: None.
    """
    if con is None:
        raise ValueError("Connection parameter is required")
    if after is None:
        raise ValueError("After parameter is required")
    try:
        # Вставка
        con.execute(
            f"""
            INSERT INTO {alias}.inventory.customers (id, first_name, last_name, email)
            VALUES (?, ?, ?, ?)
            """,
            (
                after["id"],
                after["first_name"],
                after["last_name"],
                after["email"],
            ),
        )
        logging.info("Row inserted successfully")
    except Exception as e:
        logging.error("Insert failed", exc_info=e)
        raise RuntimeError(e)


def update_record_from_pg(
        con: duckdb.DuckDBPyConnection | None,
        after: dict[str, str] | None,
        alias: str | None = "pg_trg"
) -> None:
    """
    UPDATE данных в inventory.customers.

    :param con: DB Client connection.
    :param after: Словарь с данными для вставки, полученный из Debezium.
    :param alias: Alias Postgres.
    :return: None.
    """
    if con is None:
        raise ValueError("Connection parameter is required")
    if after is None:
        raise ValueError("After parameter is required")
    try:
        # Обновление
        con.execute(
            f"""
            UPDATE {alias}.inventory.customers
            SET
                first_name = ?,
                last_name = ?,
                email = ?
            WHERE id = ?
            """,
            (
                after["first_name"],
                after["last_name"],
                after["email"],
                after["id"],
            ),
        )
        logging.info("Row updated successfully")
    except Exception as e:
        logging.error("Update failed", exc_info=e)
        raise RuntimeError(e)


def delete_record_from_pg(
        con: duckdb.DuckDBPyConnection | None,
        before: dict[str, str] | None,
        alias: str | None = "pg_trg"
) -> None:
    """
    DELETE данных в inventory.customers.

    :param alias: Alias Postgres.
    :param con: DB Client connection.
    :param before: Словарь с данными для вставки, полученный из Debezium.
    :return: None.
    """
    if con is None:
        raise ValueError("Connection parameter is required")
    if before is None:
        raise ValueError("Before parameter is required")
    try:
        # Удаление
        con.execute(
            f"""
            DELETE FROM {alias}.inventory.customers
            WHERE id = ?
            """,
            (
                before["id"],
            ),
        )
        logging.info("Row deleted successfully")
    except Exception as e:
        logging.error("Delete failed", exc_info=e)
        raise RuntimeError(e)


def manipulate_record(
        con: duckdb.DuckDBPyConnection | None,
        payload: dict[str, str | dict[str, int | str]] | None,
        trg: bool | None = True,
        alias: str | None = "pg_trg"
) -> None:
    """
    В зависимости от типа операции, полученной из Debezium выполняется INSERT, UPDATE, DELETE.

    :param con: DB Client connection.
    :param payload: Данные из Debezium.
    :param trg: Целевое или исходное Postgres.
    :param alias: Alias Postgres.
    :return: None.
    """
    if con is None:
        raise ValueError("Connection parameter is required")
    if payload is None:
        raise ValueError("Payload parameter is required")
    if trg is None:
        raise ValueError("Target parameter is required")
    if alias is None:
        raise ValueError("Alias parameter is required")
    # Получаем тип операции: c - insert, r - snapshot (начальное чтение), u - update, d - delete
    option = payload["op"]
    # В зависимости от типа операции создаем, обновляем или удаляем строку.
    match option:
        case "c" | "r":
            insert_record_to_pg(con, payload["after"], alias)
        case "u":
            update_record_from_pg(con, payload["after"], alias)
        case "d":
            delete_record_from_pg(con, payload["before"], alias)
        case _:
            raise RuntimeError(f"Unknown option: {option}")


if __name__ == "__main__":
    init_postgres_trg()