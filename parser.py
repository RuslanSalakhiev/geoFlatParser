from database.db import  create_tables
import schedule
import time
from flatParser.flatParser import run_parser


def parser_schedule():
    schedule.every().day.at("18:00").do(run_parser)
    # schedule.every(30).seconds.do(run_parser)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    create_tables()
    parser_schedule()
