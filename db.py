import sqlite3
import logging

# Set up logging
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO)


def update_db(data):
    # Establish a connection to the SQLite database, creating the file if it doesn't already exist
    conn = sqlite3.connect('flats.db')
    c = conn.cursor()

    # Create a new table called 'flats' if it doesn't already exist with specific columns for property details
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
        hide INTEGER
    )
    ''')

    insert_count = 0  # Counter for inserts
    update_count = 0  # Counter for updates

    # Attempt to insert new data into the 'flats' table
    try:
        for item in data:
            # Execute a query to check if a record with the same 'link' already exists
            c.execute('SELECT COUNT(*) FROM flats WHERE link = ?', (item['link'],))
            if c.fetchone()[0] == 0:  # If no existing record, the link is unique
                # Insert new record into the database since the link is unique
                c.execute('''
                INSERT INTO flats (link, date,first_date, district, price,first_price, floor, rooms, bedrooms, size, address, hide) 
                VALUES (:link, :date,:date, :district, :price,:price, :floor, :rooms, :bedrooms, :size, :address, 0 )
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
        logging.ERROR("An error occurred:", e)  # Print any errors that occur
        logging.info(
            f"Total inserts: {insert_count}, Total updates: {update_count}")
    finally:
        conn.close()  # Ensure the database connection is closed
        logging.info(
            f"Total inserts: {insert_count}, Total updates: {update_count}")


def check_db():
    # Connect to the SQLite database
    conn = sqlite3.connect('flats.db')
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
