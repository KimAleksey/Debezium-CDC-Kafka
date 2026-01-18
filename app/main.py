import multiprocessing
import subprocess
import sys

from app.utils.postgres import init_postgres_trg


def run_script(module_name):
    subprocess.run([sys.executable, "-m", module_name])


def main():
    # Выполнит первичную настройку целевой Postgres. Создание схемы и таблицы.
    init_postgres_trg()

    # Consumer
    script1 = "app.consumer.consumer"
    # Generator
    script2 = "app.utils.generate"

    # Создаём процессы
    p1 = multiprocessing.Process(target=run_script, args=(script1,))
    p2 = multiprocessing.Process(target=run_script, args=(script2,))

    # Запускаем процессы
    p1.start()
    p2.start()

    # Ждем завершения процессов
    p1.join()
    p2.join()

if __name__ == "__main__":
    main()