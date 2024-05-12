import os

from database.db import  create_tables
import schedule
import time
from flatParser.flatParser import run_parser


def parser_schedule():
    env = os.getenv('ENV', 'development')
    if env == 'production':
        schedule.every().day.at("09:00").do(run_parser)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        run_parser()


if __name__ == "__main__":
    create_tables()
    parser_schedule()
