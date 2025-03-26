import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()
csv_path = "../data_source/booking.csv"

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        if connection.is_connected():
            print("Connected to MySQL.")
            return connection
    except mysql.connector.Error as e:
        print("Connection error:", e)
        return None

def create_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bookings (
        booking_id BIGINT PRIMARY KEY,
        user_id BIGINT,
        apartment_id BIGINT,
        booking_date DATE,
        checkin_date DATE,
        checkout_date DATE,
        total_price FLOAT,
        currency VARCHAR(10),
        booking_status VARCHAR(50)
    );
    """
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'bookings' ready.")

def insert_batch(connection, data):
    insert_query = """
        INSERT INTO bookings (
            booking_id, user_id, apartment_id, booking_date, checkin_date, checkout_date, total_price, currency, booking_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            user_id=VALUES(user_id),
            apartment_id=VALUES(apartment_id),
            booking_date=VALUES(booking_date),
            checkin_date=VALUES(checkin_date),
            checkout_date=VALUES(checkout_date),
            total_price=VALUES(total_price),
            currency=VALUES(currency),
            booking_status=VALUES(booking_status);
    """
    records = [tuple(x) for x in data.astype(object).where(pd.notnull(data), None).values]
    batch_size = 1000
    with connection.cursor() as cursor:
        for i in range(0, len(records), batch_size):
            cursor.executemany(insert_query, records[i:i+batch_size])
            connection.commit()
            print(f"Inserted {i + len(records[i:i+batch_size])} records so far...")

def main():
    connection = connect_to_database()
    if connection:
        try:
            create_table(connection)
            for chunk in pd.read_csv(csv_path, chunksize=5000):
                insert_batch(connection, chunk)
            print("All bookings inserted successfully.")
        finally:
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()
