from threading import Thread
from database.db import  create_tables

from flatParser.scheduler import start_schedule


def run_scheduler():
    start_schedule()


if __name__ == "__main__":
    create_tables()
    scheduler_thread = Thread(target=run_scheduler)
    scheduler_thread.start()