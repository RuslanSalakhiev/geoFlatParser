from telegram import Bot, InputMediaPhoto

import asyncio

from config import bot_token, chat_id

bot = Bot(token=bot_token)


async def send_message_to_telegram(item):
    message = f"\nDate: {item['date']}" \
              f"\nPrice: {item['price']}" \
              f"\n\nDistrict: {item['district']} " \
              f"\naddress: {item['address']}" \
              f"\n\nRooms: {item['rooms']}" \
              f"\nSize: {item['size']}"\
              f"\nBedrooms: {item['bedrooms']}" \
              f"\nFloor: {item['floor']}" \
              f"\n\n[Link]({item['link']})"

    await bot.send_message(chat_id=chat_id, text=message, parse_mode='markdown')
    await asyncio.sleep(10)
    print(item)
    images = item['images_list']

    for img in images:
        await send_img_telegram(img)


    # media_group = []
    # for image_path in imgs:
    #     media_group.append(InputMediaPhoto(media=image_path, caption=message))
    #
    # await bot.send_media_group(chat_id=chat_id, media=media_group)



async def send_img_telegram(img):
    await bot.send_message(chat_id=chat_id, text=f"[source]({img})", parse_mode='markdown')


