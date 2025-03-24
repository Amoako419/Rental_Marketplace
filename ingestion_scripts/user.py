import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()
csv_path = "../data_source/user_viewings.csv"

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
    CREATE TABLE IF NOT EXISTS user_viewings (
        user_id BIGINT,
        apartment_id BIGINT,
        viewed_at DATETIME,
        is_wishlisted CHAR(1),
        call_to_action VARCHAR(100),
        PRIMARY KEY (user_id, apartment_id, viewed_at)
    );
    """
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'user_viewings' ready.")

def insert_batch(connection, data):
    # Convert viewed_at to string format that MySQL understands
    data['viewed_at'] = pd.to_datetime(data['viewed_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    insert_query = """
        INSERT INTO user_viewings (
            user_id, apartment_id, viewed_at, is_wishlisted, call_to_action
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            is_wishlisted=VALUES(is_wishlisted),
            call_to_action=VALUES(call_to_action);
    """

    # Convert to list of tuples with None for NaN values
    records = list(data.replace({pd.NA: None}).itertuples(index=False, name=None))

    batch_size = 1000
    with connection.cursor() as cursor:
        for i in range(0, len(records), batch_size):
            batch_records = records[i:i+batch_size]
            cursor.executemany(insert_query, batch_records)
            connection.commit()
            print(f"Inserted {i + len(batch_records)} records so far...")
    print("Batch inserted successfully.")

def main():
    connection = connect_to_database()
    if connection:
        try:
            create_table(connection)
            for chunk in pd.read_csv(csv_path, chunksize=5000):
                insert_batch(connection, chunk)
            print("All user_viewings inserted successfully.")
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()