import duckdb
import logging

from app.utils.utils import get_secret

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def connect_to_postgres(trg: bool | None = True, alias: str | None = "pg") -> duckdb.DuckDBPyConnection:
    """
    Подключение к DB Postgres. По-умолчанию подключается к целевому Postgres.

    :param trg: Флаг подключения к целевому Postgres. Иначе - к источнику.
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

    :param con: DB Client connection.
    :return: None.
    """
    con.execute(sql_query)


def init_postgres_trg() -> None:
    """
    Выполняет первичную настройку целевой DB. Создаем схему и таблицу.

    :return: None.
    """
    alias = "pg_trg"
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


if __name__ == "__main__":
    init_postgres_trg()