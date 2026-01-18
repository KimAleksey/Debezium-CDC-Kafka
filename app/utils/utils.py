import logging

from os import getenv
from dotenv import load_dotenv
from pathlib import Path

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def get_secret(param: str | None, log: bool | None = True) -> str:
    """
    Возвращает имя топика из .env файла.

    :return: topic name.
    """
    if param is None:
        raise ValueError("Parameter is required")
    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        raise FileNotFoundError(f"Environment file {env_path} not found")
    # URL schema registry
    secret = getenv(f"{param}")
    if log:
        logging.info(f"{param}: {secret}")
    return secret