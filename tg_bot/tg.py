import json
import logging

from telegram import Bot, InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton, Update
import asyncio
from config import bot_token, chat_id
from database.db import add_tg_message_to_db, get_average_ppm, get_district_average_ppm, get_tg_message_by_id, \
    hide_flat, update_tg_message_in_db
import requests
from telegram.ext import Application, CallbackQueryHandler

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


async def send_flat_to_telegram(item, ppm30, ppm90, ppm_district):
    size = float(item['size'].split()[0])
    price = float(item['price'].replace(',', ''))
    first_price = float(item['first_price'].replace(',', ''))

    price_arrow = ""
    if price > first_price:
        price_arrow = "⬆️"
    elif price < first_price:
        price_arrow = "⬇️"

    price_string = item['price'] if price == first_price else f"{item['price']}. <b>First:</b> {item['first_price']} {price_arrow}"
    ppm = round(price / size)
    prev_ppm = round(first_price / size)
    ppm_string = ppm if price == first_price else f"{ppm}. <b>First:</b> {prev_ppm} {price_arrow}"
    text = f"\n<b>Date</b>: {item['date']}" \
           f"\n<b>Price</b>: {price_string}" \
           f"\n<b>Price per Meter</b>: {ppm_string} " \
           f"\n" \
           f"\n<b>vs 30d avg</b>: {format_difference(ppm, ppm30)} " \
           f"\n<b>vs 90d avg</b>: {format_difference(ppm, ppm90)} " \
           f"\n<b>vs district avg</b>: {format_difference(ppm, ppm_district)} " \
           f"\n" \
           f"\n\n<b>District</b>: {item['district']} " \
           f"\n<b>address</b>: {item['address']}" \
           f"\n\n<b>Rooms</b>: {item['rooms']}" \
           f"\n<b>Size</b>: {item['size']}" \
           f"\n<b>Bedrooms</b>: {item['bedrooms']}" \
           f"\n<b>Floor</b>: {item['floor']}" \
           f"\n\n ID: {item['id']}"

    images = item['images']
    images_list = json.loads(images)

    base_url = images_list[0].rsplit('_', 1)[0] + '_'
    extension = images_list[0].split('.')[-1]

    media = []
    i = 1
    while True:
        url = f"{base_url}{i}.{extension}"
        response = requests.head(url)
        if response.status_code != 200 or i > 8:
            break
        media.append(InputMediaPhoto(media=url.replace('large', 'thumbs')))
        await asyncio.sleep(3)
        i += 1
    if media:
        try:
            sent_messages = await bot.send_media_group(chat_id=chat_id, caption=text, parse_mode='html', media=media)
            await asyncio.sleep(3)
            message_id = sent_messages[0].message_id
            message = {'id': message_id, 'text': text}
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("Link", url=item['link']),
                 InlineKeyboardButton("🫣 Hide", callback_data=f"hide_{item['id']}_{message_id}")],
                [InlineKeyboardButton("❤️ Like", callback_data=f"like_{item['id']}_{message_id}"),
                 InlineKeyboardButton("💔️ Dis", callback_data=f"dis_{item['id']}_{message_id}")]
            ])

            await bot.send_message(chat_id=chat_id, text="Actions", parse_mode='html', reply_markup=keyboard)
            await add_tg_message_to_db(message)
            await asyncio.sleep(2)
        except Exception as e:
            print(f"An error occurred while retrieving the message: {e}")


async def run_bot(item):
    ppm30 = get_average_ppm('30')
    ppm90 = get_average_ppm('90')
    ppm_district = get_district_average_ppm(item['district'])

    logging.info(f'Send Message - {item["link"]}')
    await send_flat_to_telegram(item, ppm30, ppm90, ppm_district)


async def hide_message(message_id, item_id):
    initial_text = get_tg_message_by_id(message_id)
    updated_text = f'<s>{initial_text}</s>'

    await hide_flat(item_id)
    await bot.edit_message_caption(chat_id=chat_id,
                                   message_id=message_id,
                                   caption=updated_text,
                                   parse_mode='HTML')


async def add_favorite_tag(message_id):
    initial_text = get_tg_message_by_id(message_id)
    updated_text = f'{initial_text}\n\n #Liked ❤❤❤'
    await bot.edit_message_caption(chat_id=chat_id,
                                   message_id=message_id,
                                   caption=updated_text,
                                   parse_mode='HTML')
    message = {'id': message_id, 'text': updated_text}
    update_tg_message_in_db(message)


async def remove_favorite_tag(message_id):
    initial_text = get_tg_message_by_id(message_id)

    updated_text = initial_text.replace('\n\n #Liked ❤❤❤', '')
    await bot.edit_message_caption(chat_id=chat_id,
                                   message_id=message_id,
                                   caption=updated_text,
                                   parse_mode='HTML')

    message = {'id': message_id, 'text': updated_text}
    update_tg_message_in_db(message)


async def button_handler(update: Update):
    query = update.callback_query

    await query.answer()

    data = query.data
    if "hide_" in data:
        item_id = data.split("_")[1]
        message_id = data.split("_")[2]
        await hide_message(message_id, item_id)

    elif "like_" in data:
        message_id = data.split("_")[2]
        await add_favorite_tag(message_id)

    elif "dis_" in data:
        message_id = data.split("_")[2]
        await remove_favorite_tag(message_id)


def listen_actions():
    # Replace 'YOUR_TOKEN_HERE' with your actual bot's token
    application = Application.builder().token(bot_token).build()

    # Register handlers
    application.add_handler(CallbackQueryHandler(button_handler))

    # Start the bot
    application.run_polling()

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
