import logging
import json
import os
import sqlite3
from datetime import datetime, timedelta,time

DATABASE_PATH = 'flats.db'

# Set up logging

env = os.getenv('ENV', 'development')

if env == 'production':
    level = logging.WARNING
else:
    level = logging.INFO

logging.basicConfig(
    filename='parser.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=level)


def update_flats(data, url_id):
    # Establish a connection to the SQLite database, creating the file if it doesn't already exist
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    insert_count = 0  # Counter for inserts
    update_count = 0  # Counter for updates

    # Attempt to insert new data into the 'flats' table
    try:
        for item in data:
            # Execute a query to check if a record with the same 'link' and other params already exists
            c.execute('''
                           SELECT COUNT(*) FROM flats 
                           WHERE link = ?
                       ''',
                      (item['link'],))
            is_not_exist = c.fetchone()[0] == 0
            c.execute('''
                            SELECT COUNT(*) FROM flats 
                            WHERE link <> ? and (size= ? and bedrooms = ? AND rooms = ? AND floor = ? AND LOWER(district) = LOWER(?))
                        ''',
                      (item['link'], item['size'], item['bedrooms'], item['rooms'], item['floor'], item['district'],))
            is_unique = c.fetchone()[0] == 0

            no_address = item['address'].lower() == item['district'].lower() or item['address'] == ' mititebuli ar aris'
            stop_address = 'zhvania' in item['address'].lower()

            item['request_id'] = url_id
            item['images'] = json.dumps(item["images_list"])
            item['add_date'] =  datetime.now()

            current_year = datetime.now().year
            dt = datetime.strptime(item['date'], '%d %b %H:%M')
            dt = dt.replace(year=current_year)
            item['parsed_date'] = dt.strftime('%Y-%m-%d %H:%M:%S')

            if no_address:
                logging.info(f"FLATS:No address, item skipped: {item['link']}")
            elif stop_address:
                logging.info(f"FLATS:stop address, item skipped: {item['link']}")
            elif is_not_exist and is_unique:


                c.execute('''
                INSERT INTO flats (link, date,first_date, district, price,first_price, floor, rooms, bedrooms, size, address, hide, request_id, images, like, sent_to_tg,seen,duplicates,add_date ) 
                VALUES (:link, :parsed_date,:parsed_date, :district, :price,:price, :floor, :rooms, :bedrooms, :size, :address, 0,:request_id,:images, 0,0, 0,0,:add_date )
                ''', item)
                logging.info(f"FLATS: Inserted in DB: {item['link']}")
                insert_count += 1
            elif is_not_exist and not is_unique:
                logging.info(f"FLATS: duplicate: {item['link']}")
                print('duplicate')

                c.execute('''
                                INSERT INTO flats (link, date,first_date, district, price,first_price, floor, rooms, bedrooms, size, address, hide, request_id, images, like, sent_to_tg,seen,duplicates, add_date ) 
                                VALUES (:link, :parsed_date,:parsed_date, :district, :price,:price, :floor, :rooms, :bedrooms, :size, :address, 0,:request_id,:images, 0,1, 0,1,:add_date )
                                ''', item)
            else:
                # Update the 'date' of the existing record where the link matches
                c.execute('UPDATE flats SET date = ?, price=? WHERE link = ?',
                          (item['parsed_date'], item['price'], item['link']))
                logging.info(f"FLATS: Updated existing link: {item['link']}")
                update_count += 1

        conn.commit()  # Commit all changes to the database

    except Exception as e:
        logging.error("An error occurred: {}".format(e))
        print('error???', item)
        logging.info(
            f"Total inserts: {insert_count}, Total updates: {update_count}")
    finally:
        conn.close()  # Ensure the database connection is closed
        logging.info(
            f"Total inserts: {insert_count}, Total updates: {update_count}")


def check_db():
    # Connect to the SQLite database
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()

    # Execute a query to count the total number of records in the 'flats' table
    c.execute('SELECT COUNT(*) FROM flats')
    count = c.fetchone()[0]

    # Print the number of records, indicating whether the table is empty or not
    if count > 0:
        print(f'The table has {count} records.')
    else:
        print('The table is empty.')

    # Close the database connection to free resources
    conn.close()


def create_tables():
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    # Create a new table called 'flats'
    c.execute('''
        CREATE TABLE IF NOT EXISTS flats (
            id INTEGER PRIMARY KEY,
            link TEXT,
            date TEXT,
            add_date TEXT,
            first_date TEXT,
            district TEXT,
            first_price TEXT,
            price TEXT,
            floor TEXT,
            rooms TEXT,
            bedrooms TEXT,
            size TEXT,
            address TEXT,
            hide INTEGER,
            request_id INTEGER,
            images TEXT,
            like INTEGER,
            sent_to_tg INTEGER,
            message_id INTEGER,
            seen INTEGER,
            duplicates TEXT 
        )
        ''')

    # Create a new table called 'requests'
    c.execute('''
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY,
                url TEXT,
                description TEXT,
                test_chat TEXT,
                prod_chat TEXT,
                like_message_id TEXT
            )
            ''')

    # Create a new table called 'tgmessages'
    c.execute('''
             CREATE TABLE IF NOT EXISTS tgmessages (
                 id INTEGER PRIMARY KEY,
                 message TEXT

             )
             ''')

    c.execute('SELECT COUNT(*) FROM requests')

    count = c.fetchone()[0]

    # Print the number of records, indicating whether the table is empty or not
    if count == 0:
        c.execute('''
               INSERT INTO requests (url, description, test_chat, prod_chat)
               VALUES 
               ('https://www.myhome.ge/en/s/iyideba-bina-Tbilisi/?Keyword=Vake-Saburtalo%2C+Old+Tbilisi&AdTypeID=1&PrTypeID=1&cities=1&districts=111.28.30.38.39.40.41.42.43.44.45.46.47.101.64.67.66&regions=4.6&CardView=2&FCurrencyID=1&FPriceFrom=50000&FPriceTo=100000&AreaSizeID=1&AreaSizeFrom=30&AreaSizeTo=70&RoomNums=1.2&FloorNums=notfirst.notlast&EstateTypeID=1&OwnerTypeID=1&RenovationID=1.6', 'Buy, 1-2rooms, 100k', 'test_chat_id','buy_chat_id'),
               ('https://www.myhome.ge/en/s/qiravdeba-bina-Tbilisi/?Keyword=Vake-Saburtalo&AdTypeID=3&PrTypeID=1&cities=1&districts=28.38.47&regions=4&CardView=2&FCurrencyID=1&FPriceFrom=1000&FPriceTo=1500&AreaSizeID=1&AreaSizeFrom=80&AreaSizeTo=150&BedRoomNums=2.3&FloorNums=notfirst.notlast&EstateTypeID=1&RoomNums=3.4', 'Rent, 1-1.8k, 80-180m',  'test_chat_id','rent_chat_id')
                        
           ''')
        conn.commit()

    conn.close()


def get_requests():
    conn = sqlite3.connect(DATABASE_PATH)  # Replace 'your_database_name.database' with your database file name
    cursor = conn.cursor()

    # Define the SQL query to fetch all URLs
    query = "SELECT id,url, description FROM requests"
    cursor.execute(query)
    # Fetch all results
    entries = cursor.fetchall()

    # Convert the list of tuples into a simple list of URLs
    result = [(entry[0], entry[1], entry[2]) for entry in entries]
    conn.close()
    return result


def get_new_flats(request_id):
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row  # Return dictionaries instead of tuples
    cursor = conn.cursor()
    query = """
            SELECT * FROM flats 
            WHERE hide = 0 
            AND request_id = ?
            AND sent_to_tg = 0
            AND duplicates = 0
        """
    cursor.execute(query, (request_id,))

    # Fetch all results that meet the criteria
    entries = cursor.fetchall()
    flats_list = [dict(row) for row in entries]

    conn.close()
    return flats_list


def get_average_ppm(days, url_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    daysShift = f"-{days} days"
    # SQL to calculate the average price per meter for flats posted in the last 30 days
    sql = f"""
    SELECT ROUND(AVG(cast(replace(price, ',','.') as float) * 10000 / CAST(SUBSTR(size, 1, INSTR(size, ' m²') - 1) AS REAL)))/10 as average_price
    FROM flats
    WHERE date >= date('now',  ?) AND request_id = ? AND duplicates = 0
    """

    try:
        # Execute the query

        cursor.execute(sql, (daysShift, url_id))
        # Fetch the result
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()


def get_district_average_ppm(district, url_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    # SQL to calculate the average price per meter for flats posted in the last 30 days
    sql = f"""
    SELECT ROUND(AVG(cast(replace(price, ',','.') as float) / CAST(SUBSTR(size, 1, INSTR(size, ' m²') - 1) AS REAL))* 10000)/10 as average_price
    FROM flats
    WHERE LOWER(district) = LOWER(?) and request_id = ? AND duplicates = 0
    """

    try:
        # Execute the query

        cursor.execute(sql, (district, url_id))
        # Fetch the result
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()


async def hide_flat(flat_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    sql = f"""
    UPDATE flats SET hide = 1, seen = 1, like = 0  WHERE id = ?
    """

    try:
        # Execute the query
        cursor.execute(sql, (flat_id,))
        conn.commit()
        return 'ok'
    except Exception as e:
        print(f"An error occurred: {e}")
        return 'error'
    finally:
        # Close the connection
        conn.close()


async def like_flat(flat_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    sql = f"""
    UPDATE flats SET like = 1, seen = 1, hide=0 WHERE id = ?
    """

    try:
        # Execute the query
        cursor.execute(sql, (flat_id,))
        conn.commit()
        return 'ok'
    except Exception as e:
        print(f"An error occurred: {e}")
        return 'error'
    finally:
        # Close the connection
        conn.close()


async def add_tg_message_to_db(message):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        # Execute the query
        cursor.execute('''
                           INSERT INTO tgmessages (id, message) 
                           VALUES (:id, :text)
                           ''', message)

        conn.commit()
        logging.info(f"TGMessages: Inserted {message['id']}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()


def update_tg_message_in_db(message):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        # Execute the update query
        cursor.execute('''
                           UPDATE tgmessages
                           SET message = :text
                           WHERE id = :id
                           ''', message)

        conn.commit()
        logging.info(f"TGMessages: Updated message with ID {message['id']}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()


def get_tg_message_by_id(message_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        # Prepare the SQL query to fetch the message text by message ID
        cursor.execute("SELECT message FROM tgmessages WHERE id = ?", (message_id,))
        # Fetch the first row from the query result
        result = cursor.fetchone()
        if result:
            return result[0]  # result[0] because fetchone() returns a tuple
        else:
            print(f"No message found with ID: {message_id}")
            return None
    except Exception as e:
        print(f"An error occurred while retrieving the message: {e}")
        return None
    finally:
        conn.close()


def get_chat_from_db(request_id, env):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    column = 'prod_chat' if env == 'production' else 'test_chat'

    try:
        # Prepare the SQL query to fetch the message text by message ID
        cursor.execute(f"SELECT {column} FROM requests WHERE id = ?", (request_id,))
        # Fetch the first row from the query result
        result = cursor.fetchone()

        if result:
            return result[0]  # result[0] because fetchone() returns a tuple
        else:
            print(f"No request found with ID: {request_id}")
            return None
    except Exception as e:
        print(f"An error occurred while retrieving the message: {e}")
        return None
    finally:
        conn.close()


async def update_sent_status(flat_id, message):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        # Execute the update query
        cursor.execute(f'''
                           UPDATE flats
                           SET sent_to_tg = 1,
                           message_id = ?
                           WHERE id = ?
                           ''', (message['id'], flat_id,))

        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()


def get_max_date(request_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        now = datetime.now()
        cursor.execute(f"SELECT max(date) FROM flats WHERE request_id = ? AND date < ?", (request_id, now))
        # Fetch the first row from the query result
        result = cursor.fetchone()
        if result[0]:
            return datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S")
        else:
            return datetime.today() - timedelta(days=7)
    except Exception as e:
        print(f"An error occurred while retrieving the message: {e}")
        return None
    finally:
        conn.close()


def get_like_message_id(request_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        # Prepare the SQL query to fetch the message text by message ID
        cursor.execute(f"SELECT like_message_id FROM requests WHERE id = ?", (request_id,))
        # Fetch the first row from the query result
        result = cursor.fetchone()
        return result[0]
    except Exception as e:
        print(f"An error occurred while retrieving the message: {e}")
        return None
    finally:
        conn.close()


async def update_like_message_id(request_id, message_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        # Execute the update query
        cursor.execute(f'''
                               UPDATE requests
                               SET like_message_id = ?
                               WHERE id = ?
                               ''', (message_id, request_id,))

        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()


def get_liked_flats(request_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        # Prepare the SQL query to fetch the message text by message ID
        cursor.execute(f"SELECT id,price, district, size,date FROM flats WHERE request_id = ? and like = 1",
                       (request_id,))
        # Fetch the first row from the query result
        result = cursor.fetchall()
        return [{"id": row[0], "price": row[1], "district": row[2], "size": row[3], "date": row[4]} for row in result]
    except Exception as e:
        print(f"An error occurred while retrieving the message: {e}")
        return None
    finally:
        conn.close()


def get_unseen_flats(request_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        # Prepare the SQL query to fetch the message text by message ID
        cursor.execute(f"SELECT id,price, district, size,date FROM flats WHERE request_id = ? and seen = 0 AND duplicates = 0",
                       (request_id,))
        # Fetch the first row from the query result
        result = cursor.fetchall()
        return [{"id": row[0], "price": row[1], "district": row[2], "size": row[3], "date": row[4]} for row in result]
    except Exception as e:
        print(f"An error occurred while retrieving the message: {e}")
        return None
    finally:
        conn.close()


def get_parser_stats(request_id, is_duplicate):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        # Prepare the SQL query to fetch the message text by message ID
        start_of_day = datetime.combine(datetime.now().date(), time.min)

        cursor.execute("""
                    SELECT count(*) 
                    FROM flats 
                    WHERE request_id = ? AND duplicates = ? AND add_date > ?
                """, (request_id, is_duplicate, start_of_day))
        # Fetch the first row from the query result
        result = cursor.fetchone()
        return result[0]
    except Exception as e:
        print(f"An error occurred while retrieving the message: {e}")
        return None
    finally:
        conn.close()

def get_description(request_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    print(request_id)
    try:
        # Prepare the SQL query to fetch the message text by message ID
        cursor.execute(f"SELECT description FROM requests WHERE id = ?",
                       (request_id,))
        # Fetch the first row from the query result
        result = cursor.fetchone()
        return result[0]
    except Exception as e:
        print(f"An error occurred while retrieving the message: {e}")
        return None
    finally:
        conn.close()
