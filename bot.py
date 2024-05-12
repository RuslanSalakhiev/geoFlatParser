import os

import asyncio
from datetime import datetime, timedelta

from database.db import get_new_flats, get_requests
from tg_bot.tg import run_bot


async def sleep_until():
    # Current time
    now = datetime.now()
    # Time for today at 21:00
    target_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    # If it's already past 21:00, set the target time for 21:00 the next day
    if now >= target_time:
        target_time += timedelta(days=1)
    # Calculate the number of seconds to sleep
    sleep_seconds = (target_time - now).total_seconds()
    # Sleep until it's time
    await asyncio.sleep(sleep_seconds)


async def bot_schedule():
    while True:
        env = os.getenv('ENV', 'development')
        if env == 'production':
            await sleep_until()
        urls = get_requests()
        for (url_id, url, description) in urls:
            new_flats = get_new_flats(url_id)
            for flat in new_flats:
                await run_bot(flat, description)


if __name__ == "__main__":
    asyncio.run(bot_schedule())