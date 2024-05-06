import schedule
import time
from flatParser.flatParser import run_parser


def start_schedule():
    # schedule.every().day.at("21:00").do(run_parser)
    schedule.every(30).seconds.do(run_parser)
    while True:
        schedule.run_pending()
        time.sleep(1)
