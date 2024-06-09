import json
import logging
import os
import re
from datetime import datetime

from telegram import Bot, InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton, Update
import asyncio
from config import bot_token, test_bot_token
from database.db import add_tg_message_to_db, get_average_ppm, get_description, get_district_average_ppm, \
    get_like_message_id, get_liked_flats, get_parser_stats, get_tg_message_by_id, \
    get_unseen_flats, hide_flat, like_flat, update_like_message_id, update_sent_status, \
    update_tg_message_in_db
import requests
from telegram.ext import Application, CallbackQueryHandler,ContextTypes


env = os.getenv('ENV', 'development')

if env == 'production':
    token = bot_token
else:
    token = test_bot_token

bot = Bot(token=token)


def format_difference(num1, num2):

    # Calculate the absolute difference
    difference = round(num1 - num2)
    # Calculate the percentage difference relative to num2
    percentage_difference = round((difference / num2 * 100)) if num2 != 0 else float('inf')  # Handle division by zero

    # Choose the arrow based on the difference
    arrow = "🟢" if difference < 0 else "🔴" if difference > 0 else "⚪"
    # Format the difference to include a sign and the arrow symbol
    formatted_difference = f"{difference:+} ({percentage_difference}%) {arrow}"
    return formatted_difference

async def send_flat_to_telegram(item, ppm30, ppm90, ppm_district, url_description,total_cnt, i, chat_id):
    size = float(item['size'].split()[0])
    price = float(item['price'].replace(',', ''))
    first_price = float(item['first_price'].replace(',', ''))

    price_arrow = ""
    if price > first_price:
        price_arrow = "⬆️"
    elif price < first_price:
        price_arrow = "⬇️"
    date_object = datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S")
    formatted_date = date_object.strftime("%d.%m.%y")

    first_date_object = datetime.strptime(item['first_date'], "%Y-%m-%d %H:%M:%S")
    formatted_first_date = first_date_object.strftime("%d.%m")

    price_string = item['price'] if price == first_price else f"{item['price']}. <b>First:</b> {item['first_price']} {price_arrow}"
    ppm = round(price / size)
    prev_ppm = round(first_price / size)
    ppm_string = ppm if price == first_price else f"{ppm}. <b>First:</b> {prev_ppm} ({formatted_first_date})"

    text = f"<i>#{url_description}</i> | #ID{item['id']} | {i} / {total_cnt}" \
           f"\n\n<b>Last date</b>: {formatted_date}" \
           f"\n<b>Price</b>: {price_string}" \
           f"\n<b>Price per Meter</b>: {ppm_string} " \
           f"\n\nComparison:" \
           f"\n<b>30days  </b>: {format_difference(ppm, ppm30)} " \
           f"\n<b>90days  </b>: {format_difference(ppm, ppm90)} " \
           f"\n<b>District </b>: {format_difference(ppm, ppm_district)} " \
           f"\n\n<b>District</b>: {item['district']} " \
           f"\n<b>address</b>: {item['address']}" \
           f"\n\n<b>Rooms</b>: {item['rooms']}" \
           f"\n<b>Size</b>: {item['size']}" \
           f"\n<b>Bedrooms</b>: {item['bedrooms']}" \
           f"\n<b>Floor</b>: {item['floor']}" \
           f"\n#Unseen"

    images = item['images']
    images_list = json.loads(images)

    media = []
    i = 1
    if images_list:

        img_type = re.search(r'\.[^.]+$', images_list[0]).group()

        if img_type != '.webp':

            base_url = images_list[0].rsplit('_', 1)[0] + '_'
            extension = images_list[0].split('.')[-1]

            while True:
                url = f"{base_url}{i}.{extension}"
                response = requests.head(url)
                if response.status_code != 200 or i > 10:
                    break
                media.append(InputMediaPhoto(media=url.replace('large', 'thumbs')))
                await asyncio.sleep(2)
                i += 1
        else:
            media.append(InputMediaPhoto(media=images_list[0]))
            await asyncio.sleep(2)
            media.append(InputMediaPhoto(media=images_list[1]))
            await asyncio.sleep(2)


    if media:
        retries = 3
        for attempt in range(retries):
            try:
                await asyncio.sleep(2)
                sent_messages = await bot.send_media_group(read_timeout=40, write_timeout=40, chat_id=chat_id, caption=text, parse_mode='html', media=media, connect_timeout=40)
                await asyncio.sleep(4)
                message_id = sent_messages[0].message_id
                message = {'id': message_id, 'text': text}
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Link", url=item['link']),
                     InlineKeyboardButton("🫣 Hide", callback_data=f"hide_{item['id']}_{message_id}_{chat_id}")],
                    [InlineKeyboardButton("❤️ Like", callback_data=f"like_{item['id']}_{message_id}_{chat_id}")]
                ])
                await add_tg_message_to_db(message)
                await update_sent_status(item['id'], message)

                actions_retries = 3
                for actions_attempt in range(actions_retries):
                    try:
                        await bot.send_message(chat_id=chat_id, text="Actionsㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤ", parse_mode='html', reply_markup=keyboard, read_timeout=15,write_timeout=15)
                        break
                    except Exception as e:
                        print(f"An error occurred while sending the message (attempt {actions_attempt + 1}/{actions_retries}): {e}")
                        if actions_attempt < actions_retries - 1:
                            await asyncio.sleep(5)  # wait before retrying
                        else:
                            print("Failed to send the message after multiple attempts.")
                    finally:
                        await asyncio.sleep(2)

                await asyncio.sleep(3)
                break
            except Exception as e:
                if attempt < retries - 1:
                    await asyncio.sleep(5)
                print(f"An error occurred while sending the message: {e}")
            finally:
                await asyncio.sleep(2)


async def run_bot(item, url_description, total_cnt, i, chat_id,url_id ):
    ppm30 = get_average_ppm('30', url_id )
    ppm90 = get_average_ppm('90', url_id)
    ppm_district = get_district_average_ppm(item['district'], url_id)

    logging.info(f'Send Message - {item["link"]}')
    await send_flat_to_telegram(item, ppm30, ppm90, ppm_district, url_description,total_cnt, i, chat_id)


async def hide_message(message_id, item_id, chat_id):
    initial_text = get_tg_message_by_id(message_id).replace('\n\n #Liked ❤❤❤', '')
    updated_text = f'<s>{initial_text}</s>'
    updated_text = updated_text.replace('\n#Unseen', '')

    await hide_flat(item_id)
    await bot.edit_message_caption(chat_id=chat_id,
                                   message_id=message_id,
                                   caption=updated_text,
                                   parse_mode='HTML')
    message = {'id': message_id, 'text': updated_text}
    update_tg_message_in_db(message)


async def add_favorite_tag(message_id, chat_id):
    initial_text = get_tg_message_by_id(message_id)
    updated_text = f'{initial_text}\n\n #Liked ❤❤❤'
    updated_text = updated_text.replace('\n#Unseen', '')
    updated_text = updated_text.replace('<s>', '')
    updated_text = updated_text.replace('</s>', '')

    await bot.edit_message_caption(chat_id=chat_id,
                                   message_id=message_id,
                                   caption=updated_text,
                                   parse_mode='HTML')
    message = {'id': message_id, 'text': updated_text}
    update_tg_message_in_db(message)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query

    await query.answer()

    data = query.data

    if "hide_" in data:
        print('hide', data)
        item_id = data.split("_")[1]
        message_id = data.split("_")[2]
        chat_id = data.split("_")[3]
        await hide_message(message_id, item_id, chat_id)

    elif "like_" in data:
        print('like',data)
        item_id = data.split("_")[1]
        message_id = data.split("_")[2]
        chat_id = data.split("_")[3]
        await add_favorite_tag(message_id, chat_id)
        await like_flat(item_id)


def listen_actions():
    application = Application.builder().token(token).build()

    # Register handlers
    application.add_handler(CallbackQueryHandler(button_handler))

    # Start the bot
    application.run_polling()


async def send_summary_message(request_id, chat_id):
    unseen_flats = get_unseen_flats(request_id)
    liked_flats = get_liked_flats(request_id)

    new_uniq_flats = get_parser_stats(request_id, 0)
    new_duplicates_flats =  get_parser_stats(request_id, 1)

    description = get_description(request_id)
    summary_lines = [f"<b>Summary</b> #{description}\n",f"Parser: New - {new_uniq_flats}, Duplicates: {new_duplicates_flats}\n"]

    summary_lines.append("Liked ❤")

    for flat in liked_flats:
        date_object = datetime.strptime(flat['date'], "%Y-%m-%d %H:%M:%S")
        formatted_date = date_object.strftime("%d.%m")
        summary_lines.append(f"#ID{flat['id']} | {flat['district']} | {flat['price']} | {flat['size']} | {formatted_date}")

    summary_lines.append(f'\n\nUnseen 👀 \n{len(unseen_flats)}  #Unseen')

    summary_string = "\n".join(summary_lines)
    summary_message = await bot.send_message(chat_id=chat_id, text=summary_string, parse_mode='Html', read_timeout=15, write_timeout=15)
    prev_message_id = get_like_message_id(request_id)
    await asyncio.sleep(2)
    if prev_message_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=prev_message_id)
        except Exception as e:
            print(f"An error occurred while sending the message: {e}")

    await asyncio.sleep(2)
    await update_like_message_id(request_id, summary_message.message_id)


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
