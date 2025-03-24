import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()
csv_path = "../data_source/apartment_attributes.csv"

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
    CREATE TABLE IF NOT EXISTS apartments_attributes (
        id BIGINT PRIMARY KEY,
        category VARCHAR(100),
        body TEXT,
        amenities TEXT,
        bathrooms FLOAT,
        bedrooms FLOAT,
        fee FLOAT,
        has_photo TEXT,
        pets_allowed VARCHAR(50),
        price_display VARCHAR(50),
        price_type VARCHAR(50),
        square_feet FLOAT,
        address VARCHAR(255),
        cityname VARCHAR(100),
        state VARCHAR(100),
        latitude FLOAT,
        longitude FLOAT
    );
    """
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'apartments_attributes' ready.")

def insert_batch(connection, data):
    insert_query = """
        INSERT INTO apartments_attributes (
            id, category, body, amenities, bathrooms, bedrooms, fee, has_photo,
            pets_allowed, price_display, price_type, square_feet, address,
            cityname, state, latitude, longitude
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            category=VALUES(category),
            body=VALUES(body),
            amenities=VALUES(amenities),
            bathrooms=VALUES(bathrooms),
            bedrooms=VALUES(bedrooms),
            fee=VALUES(fee),
            has_photo=VALUES(has_photo),
            pets_allowed=VALUES(pets_allowed),
            price_display=VALUES(price_display),
            price_type=VALUES(price_type),
            square_feet=VALUES(square_feet),
            address=VALUES(address),
            cityname=VALUES(cityname),
            state=VALUES(state),
            latitude=VALUES(latitude),
            longitude=VALUES(longitude);
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
            print("All apartment attributes inserted successfully.")
        finally:
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()
