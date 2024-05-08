import json
import logging
from telegram import Bot
import asyncio
from config import bot_token, chat_id
from database.db import get_average_ppm

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

    message = f"\nDate: {item['date']}" \
              f"\nPrice: {item['price']}" \
              f"\nPrice per Meter: {ppm} " \
              f"\nPPM vs avg-30: {format_difference(ppm,ppm30)} " \
              f"\nPPM vs avg-90: {format_difference(ppm,ppm90)} " \
              f"\n" \
              f"\n\nDistrict: {item['district']} " \
              f"\naddress: {item['address']}" \
              f"\n\nRooms: {item['rooms']}" \
              f"\nSize: {item['size']}"\
              f"\nBedrooms: {item['bedrooms']}" \
              f"\nFloor: {item['floor']}" \
              f"\n" \
              f"\n\n[Link]({item['link']}), [Hide]({item['link']})"\

    await bot.send_message(chat_id=chat_id, text=message, parse_mode='markdown')
    await asyncio.sleep(10)
    images = item['images']
    images_list = json.loads(images)

    for image in images_list:
        await send_img_telegram(image)


async def send_img_telegram(img):
    await bot.send_message(chat_id=chat_id, text=f"[source]({img})", parse_mode='markdown')
    await asyncio.sleep(5)

async def run_bot(item):
    ppm30 = get_average_ppm('30')
    ppm90 = get_average_ppm('90')
    logging.info(f'Send Message - {item["link"]}')
    await send_flat_to_telegram(item,ppm30,ppm90)
