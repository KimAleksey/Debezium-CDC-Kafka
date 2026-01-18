import multiprocessing
import subprocess

from app.utils.postgres import init_postgres_trg


def run_script(script_path):
    subprocess.run(["python", script_path])


def main():
    # Выполнит первичную настройку целевой Postgres. Создание схемы и таблицы.
    init_postgres_trg()

    # Consumer
    script1 = "./consumer/consumer.py"
    # Generator
    script2 = "./utils/generate.py"

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