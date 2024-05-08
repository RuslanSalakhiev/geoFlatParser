import logging
import json
import sqlite3
from datetime import datetime

DATABASE_PATH = 'flats.db'

# Set up logging
logging.basicConfig(
    filename='parser.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO)


def update_flats(data, url_id):
    # Establish a connection to the SQLite database, creating the file if it doesn't already exist
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    insert_count = 0  # Counter for inserts
    update_count = 0  # Counter for updates

    # Attempt to insert new data into the 'flats' table
    try:
        for item in data:
            # Execute a query to check if a record with the same 'link' already exists
            c.execute('SELECT COUNT(*) FROM flats WHERE link = ?', (item['link'],))
            if c.fetchone()[0] == 0:  # If no existing record, the link is unique
                # Insert new record into the database since the link is unique
                item['request_id'] = url_id
                item['images'] = json.dumps(item["images_list"])
                c.execute('''
                INSERT INTO flats (link, date,first_date, district, price,first_price, floor, rooms, bedrooms, size, address, hide, request_id, images ) 
                VALUES (:link, :date,:date, :district, :price,:price, :floor, :rooms, :bedrooms, :size, :address, 0,:request_id,:images  )
                ''', item)
                logging.info(f"Inserted in DB: {item['link']}")
                insert_count += 1


            else:
                # Update the 'date' of the existing record where the link matches
                c.execute('UPDATE flats SET date = ?, price=? WHERE link = ?',
                          (item['date'], item['price'], item['link']))
                logging.info(f"Updated existing link: {item['link']}")
                update_count += 1

        conn.commit()  # Commit all changes to the database

    except Exception as e:
        logging.error("An error occurred: {}".format(e))

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
            images TEXT
        )
        ''')

    # Create a new table called 'requests'
    c.execute('''
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY,
                url TEXT,
                description TEXT
            )
            ''')

    c.execute('SELECT COUNT(*) FROM requests')

    count = c.fetchone()[0]

    # Print the number of records, indicating whether the table is empty or not
    if count == 0:
        c.execute('''
               INSERT INTO requests (url, description)
                       VALUES ('https://www.myhome.ge/en/s/iyideba-bina-Tbilisi/?Keyword=Vake-Saburtalo%2C+Old+Tbilisi&AdTypeID=1&PrTypeID=1&cities=1&districts=111.28.30.38.39.40.41.42.43.44.45.46.47.101.64.67.66&regions=4.6&CardView=2&FCurrencyID=1&FPriceFrom=50000&FPriceTo=100000&AreaSizeID=1&AreaSizeFrom=30&AreaSizeTo=70&RoomNums=1.2&FloorNums=notfirst.notlast&EstateTypeID=1&OwnerTypeID=1&RenovationID=1.6', 'buy, vake-saburtalo, 1-2rooms, <100k')
           ''')
        conn.commit()

    conn.close()


def get_requests():
    conn = sqlite3.connect(DATABASE_PATH)  # Replace 'your_database_name.database' with your database file name
    cursor = conn.cursor()

    # Define the SQL query to fetch all URLs
    query = "SELECT id,url FROM requests"
    cursor.execute(query)
    # Fetch all results
    entries = cursor.fetchall()

    # Convert the list of tuples into a simple list of URLs
    result = [(entry[0], entry[1]) for entry in entries]
    conn.close()
    return result


def get_new_flats(request_id):
    conn = sqlite3.connect(DATABASE_PATH)
    # This line changes the row factory method to return dictionaries instead of tuples
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    # Define the SQL query to fetch flats that are not hidden, match the request_id, and have today's date
    query = """
      SELECT * FROM flats 
      WHERE hide = 0 
      AND request_id = ?
      """
    cursor.execute(query, (request_id,))
    # Fetch all results
    entries = cursor.fetchall()

    today_date = datetime.now().strftime("%d %b")  # Formats the date as 'Day MonthName'

    # Filter entries where date matches today's date
    flats_list = [dict(row) for row in entries if
                  datetime.strptime(row['date'], "%d %b %H:%M").strftime("%d %b") == today_date]
    # Convert rows to dictionaries

    conn.close()
    return flats_list


def get_average_ppm(days):
    conn = sqlite3.connect('flats.db')
    cursor = conn.cursor()
    daysShift = f"-{days} days"
    # SQL to calculate the average price per meter for flats posted in the last 30 days
    sql = f"""
    SELECT ROUND(AVG(price / CAST(SUBSTR(size, 1, INSTR(size, ' m²') - 1) AS REAL))* 1000) as average_price
    FROM flats
    WHERE date >= date('now',  '{daysShift}')
    """

    try:
        # Execute the query

        cursor.execute(sql)
        # Fetch the result
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()