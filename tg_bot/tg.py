import json
import logging
import time

from telegram import Bot, InputMediaPhoto
import asyncio
from config import bot_token, chat_id, vm_ip, vm_port
from database.db import add_tg_message_to_db, get_average_ppm, get_tg_message_by_id
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
    text = f"\n<b>Date</b>: {item['date']}" \
              f"\n<b>Price</b>: {item['price']}" \
              f"\n<b>Price per Meter</b>: {ppm} " \
              f"\n<b>PPM vs *avg-30</b>: {format_difference(ppm,ppm30)} " \
              f"\n<b>PPM vs avg-90</b>: {format_difference(ppm,ppm90)} " \
              f"\n" \
              f"\n\n<b>District</b>: {item['district']} " \
              f"\n<b>address</b>: {item['address']}" \
              f"\n\n<b>Rooms</b>: {item['rooms']}" \
              f"\n<b>Size</b>: {item['size']}"\
              f"\n<b>Bedrooms</b>: {item['bedrooms']}" \
              f"\n<b>Floor</b>: {item['floor']}" \
              f"\n" \
              f"\n<a href='{item['link']}'>Link</a>, <a href='{hide_link}'>Hide</a>"

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
        sent_messages = await bot.send_media_group(chat_id=chat_id, caption=text, parse_mode='html', media=media)
        message_id = sent_messages[0].message_id
    else:
        sent_message = await bot.send_message(chat_id=chat_id, text=text, parse_mode='html')
        message_id = sent_message.message_id

    message = {'id': message_id, 'text': text}
    add_tg_message_to_db(message)
    await asyncio.sleep(2)
    await add_message_id(message_id, hide_link)
    await asyncio.sleep(10)



async def run_bot(item):
    ppm30 = get_average_ppm('30')
    ppm90 = get_average_ppm('90')
    logging.info(f'Send Message - {item["link"]}')
    await send_flat_to_telegram(item,ppm30,ppm90)


async def hide_message(message_id):
    initial_text = get_tg_message_by_id(message_id)

    updated_text = f'<s>{initial_text}</s>'

    await bot.edit_message_caption(chat_id=chat_id,
                                   message_id=message_id,
                                   caption=updated_text,
                                   parse_mode='HTML')




async def add_message_id(message_id, hide_link):
    initial_text = get_tg_message_by_id(message_id)

    new_hide_link = hide_link + f"&message_id={message_id}"
    updated_text = initial_text.replace(hide_link, new_hide_link)

    await bot.edit_message_caption(chat_id=chat_id,
                                   message_id=message_id,
                                   caption=updated_text,
                                   parse_mode='HTML')
    await asyncio.sleep(2)

# async def run_test():
#
#     media = []
#
#     # media_1 = InputMediaPhoto(media='https://static.my.ge/myhome/photos/7/9/1/1/8/thumbs/18181197_1.jpg?v=8')
#     media_2 = InputMediaPhoto(media='https://static.my.ge/myhome/photos/8/2/9/2/4/thumbs/17942928_3.jpg?v=7')
#
#     # media_3 = InputMediaPhoto(media='https://static.my.ge/myhome/photos/4/5/2/6/8/large/18086254_1.jpg?v=10')
#     # media.append(media_1)
#     media.append(media_2)
#
#
#     # media.append(media_3)
#
#     text = f"<s>\n<b>Date</b>\\~" \
#            f"\n*Date*: " \
#            f"\n" \
#            f"\n*Date*: " \
#            f"\n" \
#            f"\n<a href='https://chatgpt.com/c/102f0cbd-4b89-4870-a5e7-ae819dd3b7cd'>Hide</a>), [Hide](https://chatgpt.com/c/102f0cbd-4b89-4870-a5e7-ae819dd3b7cd)</s>"
#
#     await bot.send_media_group(chat_id=chat_id,caption=text, media=media, parse_mode='html')
#     await asyncio.sleep(5)
