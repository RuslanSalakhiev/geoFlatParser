import threading
import time
from parser.parser import parse
from db.db import update_flats, create_tables, get_requests
import asyncio
import schedule

from tg_bot.tg import send_message_to_telegram


def parser_job():
    urls = get_requests()
    for url_id, url in urls:
        data = parse(url)
        asyncio.run(update_flats(data, url_id))


def run_parser():
    schedule.every().day.at("21:00").do(parser_job)
    # schedule.every().minute.do(parser_job)
    # parser_job()
    while True:
        schedule.run_pending()
        time.sleep(60)  # sleep for a minute to avoid excessive CPU use


def main():
    create_tables()
    parser_thread = threading.Thread(target=run_parser)
    parser_thread.start()
    asyncio.run(send_message_to_telegram('a'))

if __name__ == "__main__":
    main()
