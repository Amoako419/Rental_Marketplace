import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()
csv_path = "../data_source/apartments.csv"

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
    CREATE TABLE IF NOT EXISTS apartments (
        id BIGINT PRIMARY KEY,
        title VARCHAR(255),
        source VARCHAR(50),
        price FLOAT,
        currency VARCHAR(10),
        listing_created_on DATETIME,
        is_active BOOLEAN,
        last_modified_timestamp DATETIME
    );
    """
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'apartments' ready.")

def insert_batch(connection, data):
    insert_query = """
        INSERT INTO apartments (
            id, title, source, price, currency, listing_created_on, is_active, last_modified_timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title=VALUES(title),
            source=VALUES(source),
            price=VALUES(price),
            currency=VALUES(currency),
            listing_created_on=VALUES(listing_created_on),
            is_active=VALUES(is_active),
            last_modified_timestamp=VALUES(last_modified_timestamp);
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
            for chunk in pd.read_csv(csv_path, chunksize=5000, parse_dates=["listing_created_on", "last_modified_timestamp"]):
                insert_batch(connection, chunk)
            print("All apartments inserted successfully.")
        finally:
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()
