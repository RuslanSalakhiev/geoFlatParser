import json
import logging
import time

from telegram import Bot, InputMediaPhoto
import asyncio
from config import bot_token, chat_id, vm_ip, vm_port
from database.db import get_average_ppm
import requests

bot = Bot(token=bot_token)


async def send_message_to_telegram(message):

    await bot.send_message(chat_id=chat_id, text=message, parse_mode='markdown')
    await asyncio.sleep(10)


def format_difference(num1, num2):
    # Calculate the difference
    difference = round(num1 - num2)
    arrow = "🟢"
    if difference > 0:
        arrow = "🔴"
    elif difference < 0:
        arrow = "🟢"
    # Format the difference to include a sign
    formatted_difference = f"{difference:+} {arrow}"
    return formatted_difference


async def send_flat_to_telegram(item, ppm30, ppm90):
    size = float(item['size'].split()[0])
    price = float(item['price'].replace(',', ''))
    ppm = round(price / size )

    hide_link = f"http://{vm_ip}:{vm_port}/update?id={item['id']}"
    message = f"\n*Date*: {item['date']}" \
              f"\n*Price*: {item['price']}" \
              f"\n*Price per Meter*: {ppm} " \
              f"\n*PPM vs avg-30*: {format_difference(ppm,ppm30)} " \
              f"\n*PPM vs avg-90*: {format_difference(ppm,ppm90)} " \
              f"\n" \
              f"\n\n*District*: {item['district']} " \
              f"\n*address*: {item['address']}" \
              f"\n\n*Rooms*: {item['rooms']}" \
              f"\n*Size*: {item['size']}"\
              f"\n*Bedrooms*: {item['bedrooms']}" \
              f"\n*Floor*: {item['floor']}" \
              f"\n" \
              f"\n[Link]({item['link']}), [Hide]({hide_link})"\

    images = item['images']
    images_list = json.loads(images)

    base_url = images_list[0].rsplit('_', 1)[0] + '_'
    extension = images_list[0].split('.')[-1]

    media = []
    i = 1
    while True:
        url = f"{base_url}{i}.{extension}"
        response = requests.head(url)
        if response.status_code != 200:
            break
        media.append(InputMediaPhoto(media=url.replace('large','thumbs')))
        i += 1
        time.sleep(2)

    if media:
        await bot.send_media_group(chat_id=chat_id, caption=message, parse_mode='markdown', media=media)
    else:
        await bot.send_message(chat_id=chat_id, text=message, parse_mode='markdown')
    await asyncio.sleep(10)



async def run_bot(item):
    ppm30 = get_average_ppm('30')
    ppm90 = get_average_ppm('90')
    logging.info(f'Send Message - {item["link"]}')
    await send_flat_to_telegram(item,ppm30,ppm90)
