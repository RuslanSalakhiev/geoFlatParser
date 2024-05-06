import time

from bs4 import BeautifulSoup
import re
import logging
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from config import url_pattern, parse_days_count
from database.db import get_new_flats, get_requests, update_flats
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# Set up logging
from tg_bot.tg import send_message_to_telegram

logging.basicConfig(
    filename='parser.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Include timestamp
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)


def get_chrome_headless():
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Ensures Chrome runs in headless mode
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
    chrome_options.add_argument("--disable-gpu")  # Applicable for GPUs
    chrome_options.add_argument("--window-size=1920x1080")  # Standard window size for most desktop screens
    # chrome_options.binary_location = chrome_path  # Update this path

    # Set the executable path and initialize the driver
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

    return driver


def transliterate_georgian(text):
    # Map of Georgian characters to Latin characters
    transliteration_map = {
        'ა': 'a', 'ბ': 'b', 'გ': 'g', 'დ': 'd', 'ე': 'e',
        'ვ': 'v', 'ზ': 'z', 'თ': 't', 'ი': 'i', 'კ': 'k',
        'ლ': 'l', 'მ': 'm', 'ნ': 'n', 'ო': 'o', 'პ': 'p',
        'ჟ': 'zh', 'რ': 'r', 'ს': 's', 'ტ': 't', 'უ': 'u',
        'ფ': 'p', 'ქ': 'k', 'ღ': 'gh', 'ყ': 'q', 'შ': 'sh',
        'ჩ': 'ch', 'ც': 'ts', 'ძ': 'dz', 'წ': 'ts', 'ჭ': 'ch',
        'ხ': 'kh', 'ჯ': 'j', 'ჰ': 'h'
    }

    # Replace each Georgian letter with its Latin counterpart
    translated_text = ''.join(transliteration_map.get(letter, letter) for letter in text)
    return translated_text


def clean_address(text):
    # Remove the word 'kucha' from the address text
    result = text.replace('kucha', '')
    result = result.replace(' k.', '')
    return result


def get_text(card, selector, default=''):
    # Use BeautifulSoup to select a single element by CSS selector and return its text, or a default if not found
    element = card.select_one(selector)
    return element.text.strip() if element else default


def transliterate_and_clean(card, selector, default=''):
    # Helper function to extract text using a selector, transliterate from Georgian, and clean address text
    text = get_text(card, selector, default)
    return clean_address(transliterate_georgian(text))


def parse_url(init_url):
    # chrome_options = Options()
    # chrome_options.binary_location = chrome_path  # Update this path
    # driver = webdriver.Chrome(options=chrome_options)
    driver = get_chrome_headless()

    data = []
    page = 1
    actual_data = True
    logging.info('Start parsing')

    while actual_data:
        logging.info(f'Page - {page}')
        # Navigate to the given URL
        url = init_url + '&Page=' + str(page)
        driver.get(url)
        driver.implicitly_wait(10)

        WebDriverWait(driver, 10).until(
            lambda driver: driver.execute_script('return document.readyState') == 'complete'
        )

        # Extract the source HTML of the page
        html_content = driver.page_source
        # Parse the HTML content using BeautifulSoup
        catalog_xml = BeautifulSoup(html_content, 'lxml')
        # Define a regex pattern for URLs of property listings
        regex_url_pattern = re.compile(url_pattern)
        # Find all link elements that match the URL pattern
        all_cards = catalog_xml.findAll('a', href=regex_url_pattern)

        logging.info(f'Total cards - {len(all_cards)}')

        for card in all_cards:

            link = card.get('href')
            card_date = get_text(card,
                                 "div.w-full.px-5 > div:nth-child(5) > div.flex.justify-between.break-all.h-6.mt-3 > div > span")

            is_vip_badge = bool(card.select('[class^="SuperVipIcon"], [class^="VipPlusIcon"],  [class^="VipIcon"]'))

            # check for today data
            date = datetime.strptime(card_date + " " + str(datetime.now().year), '%d %b %H:%M %Y')
            today = datetime.today()
            start_date = today - timedelta(days=parse_days_count)
            logging.info(f"Link - {link}, Date - {date},  {'VIP' if is_vip_badge else ''}, ")

            if date < start_date and not is_vip_badge:
                actual_data = False
                break

            # Extract various pieces of data for each property listing

            images = card.findAll('img')
            images_list = [img.get('src') for img in images if img.get('src') is not None]

            entity = {
                "link": link,
                "date": card_date,
                "district": transliterate_and_clean(card,
                                                    "div.w-full.px-5 > div:nth-child(5) > div.flex.justify-between.break-all.h-6.mt-3 > span"),
                "price": get_text(card,
                                  "div.w-full.px-5 > div.mt-3\.5.flex.h-5.items-center.gap-2\.5.mt-4 > div.flex.items-center.overflow-hidden.font-tbcx-medium.gap-1 > span"),
                "floor": get_text(card,
                                  "div.w-full.px-5 > div.h-5.mt-3 > div > div.group.flex.items-center.gap-2> div.flex.items-center.gap-2.whitespace-nowrap.transition-all.hover\:text-secondary-100 > span"),
                "rooms": get_text(card, "div.w-full.px-5 > div.h-5.mt-3 > div > div:nth-child(2)> div > span"),
                "bedrooms": get_text(card, "div.w-full.px-5 > div.h-5.mt-3 > div > div:nth-child(3) > div > span"),
                "size": get_text(card, "div.w-full.px-5 > div.h-5.mt-3 > div > div:nth-child(4) > div > span"),
                "address": transliterate_and_clean(card, "div.w-full.px-5 > div.mt-3.line-clamp-1 > div > p"),
                "images_list": images_list
            }

            data.append(entity)

        page += 1
    logging.info('End of parsing')
    return data


def run_parser():
    urls = get_requests()
    for url_id, url in urls:
        data = parse_url(url)
        update_flats(data, url_id)

        new_flats = get_new_flats(url_id)
        send_message_to_telegram('a')
        # for flat in new_flats:
        #     send_message_to_telegram(flat['link'])