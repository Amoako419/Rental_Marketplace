import sys
import boto3
import redshift_connector
import pandas as pd
import numpy as np
import logging
from awsglue.utils import getResolvedOptions
from io import StringIO
import re
import io

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Parse arguments from Glue job parameters
args = getResolvedOptions(sys.argv, [
    'AWS_REGION',
    'S3_BUCKET',
    'S3_PREFIX',
    'REDSHIFT_HOST',
    'CURATED_DB',  # Database for curated data
    'PRESENTATION_DB',  # Database for presentation data
    'REDSHIFT_USER',
    'REDSHIFT_PASSWORD',
    'IAM_ROLE'
])

# Extract parameters
AWS_REGION = args['AWS_REGION']
S3_BUCKET = args['S3_BUCKET']
S3_PREFIX = args['S3_PREFIX']
REDSHIFT_HOST = args['REDSHIFT_HOST']
CURATED_DB = args['CURATED_DB']  # Curated database
PRESENTATION_DB = args['PRESENTATION_DB']  # Presentation database
REDSHIFT_USER = args['REDSHIFT_USER']
REDSHIFT_PASSWORD = args['REDSHIFT_PASSWORD']
IAM_ROLE = args['IAM_ROLE']

# Initialize S3 client
s3_client = boto3.client('s3', region_name=AWS_REGION)

# Function to establish Redshift connection
def get_redshift_connection(database):
    try:
        conn = redshift_connector.connect(
            host=REDSHIFT_HOST,
            database=database,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
        logging.info(f"Connected to Redshift database: {database}")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Redshift: {str(e)}")
        sys.exit(1)

# Function to read data from Redshift
def read_from_redshift(query, database):
    try:
        conn = get_redshift_connection(database)
        df = pd.read_sql(query, conn)
        conn.close()
        logging.info(f"Successfully read data from {database}: {query}")
        return df
    except Exception as e:
        logging.error(f"Error reading from Redshift: {str(e)}")
        return pd.DataFrame()

# Function to write data to S3
def write_to_s3(df, s3_key):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer.getvalue())
        logging.info(f"Successfully wrote data to S3: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logging.error(f"Error writing to S3: {str(e)}")


def read_from_s3(s3_key):
    try:
        logging.info(f"Attempting to read from S3: s3://{S3_BUCKET}/{s3_key}")
        # Fetch object from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        # Check if response contains data
        if "Body" not in response:
            logging.error(f"No 'Body' in response for s3://{S3_BUCKET}/{s3_key}")
            return pd.DataFrame()
        # Read CSV into DataFrame
        df = pd.read_csv(response["Body"], encoding="utf-8")
        # Log number of rows and columns
        logging.error(f"Successfully read data from S3: s3://{S3_BUCKET}/{s3_key}") #### Change it back to info 
        logging.info(f"DataFrame shape: {df.shape}")
        logging.info(f"Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logging.error(f"Error reading from S3: {str(e)}")
        return pd.DataFrame()


def bulk_load_to_redshift(df, table_name, database, s3_staging_dir):
    try:
        # Prepare connection
        conn = get_redshift_connection(database)
        cursor = conn.cursor()
        # Prepare S3 staging path
        s3_key = f"{s3_staging_dir}/{table_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        # Convert DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=True)
        csv_buffer.seek(0)
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET, 
            Key=s3_key, 
            Body=csv_buffer.getvalue()
        )
        
        # Construct COPY command
        copy_cmd = f"""
        COPY {table_name}
        FROM 's3://{S3_BUCKET}/{s3_key}'
        IAM_ROLE '{IAM_ROLE}'
        CSV
        IGNOREHEADER 1
        """
        
        # Execute COPY command
        cursor.execute(copy_cmd)
        conn.commit()
        
        logging.info(f"Bulk loaded {table_name} successfully via COPY command")
        
    except Exception as e:
        logging.error(f"Error in bulk loading {table_name}: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def calculate_avg_listing_price_per_week(df):
    try:
        if 'is_active' in df.columns:
            df['is_active'] = df['is_active'].astype(str).str.lower().map({'true': True, 'false': False})
        df = df[df['is_active'] == True]
        df['listing_created_on'] = pd.to_datetime(df['listing_created_on'])
        df['week_start'] = df['listing_created_on'].dt.to_period('W').apply(lambda r: r.start_time)
        return df.groupby('week_start').agg(avg_listing_price=('price', 'mean')).reset_index()
        logging.info("Calculated Average Listing Price Per Week.")
    except Exception as e:
        logging.error(f"Error calculating Average Listing: {str(e)}")
        return pd.DataFrame()
    

# Function to calculate Occupancy Rate
def calculate_occupancy_rate(bookings, apartments):
    try:
        bookings['checkin_date'] = pd.to_datetime(bookings['checkin_date'])
        bookings['checkout_date'] = pd.to_datetime(bookings['checkout_date'])
        bookings['booked_nights'] = (bookings['checkout_date'] - bookings['checkin_date']).dt.days
        confirmed_bookings = bookings[bookings['booking_status'] == 'confirmed']
        confirmed_bookings['month'] = confirmed_bookings['checkin_date'].dt.to_period('M').apply(lambda r: r.start_time)
        total_booked_nights = confirmed_bookings.groupby('month').agg(total_booked_nights=('booked_nights', 'sum')).reset_index()
        active_apartment_count = len(apartments[apartments['is_active'] == True])
        available_nights = total_booked_nights[['month']].copy()
        available_nights['days_in_month'] = available_nights['month'].dt.daysinmonth
        available_nights['total_available_nights'] = available_nights['days_in_month'] * active_apartment_count
        occupancy_df = total_booked_nights.merge(available_nights, on='month')
        occupancy_df['occupancy_rate'] = (occupancy_df['total_booked_nights'] / occupancy_df['total_available_nights']) * 100
        return occupancy_df
        logging.info("Calculated occupancy Rate")
    except Exception as e:
        logging.error(f"Error calculating Occupancy Rate: {str(e)}")
        return pd.DataFrame()
    
    
def calculate_top_performing_listings_per_week(bookings):
    try:
        confirmed_bookings = bookings[bookings['booking_status'] == 'confirmed'].copy()
        confirmed_bookings['booking_date'] = pd.to_datetime(confirmed_bookings['booking_date'])
        confirmed_bookings['week'] = confirmed_bookings['booking_date'].dt.to_period('W').apply(lambda r: r.start_time)

        # Compute total revenue per listing per week
        weekly_revenue = confirmed_bookings.groupby(['week', 'apartment_id'])['total_price'].sum().reset_index()
        weekly_revenue.rename(columns={'total_price': 'total_revenue'}, inplace=True)

        # Get the top-performing listing per week
        top_performing_listings = weekly_revenue.loc[
            weekly_revenue.groupby('week')['total_revenue'].idxmax()
        ].reset_index(drop=True)

        logging.info("Calculated Top Performing Listings Per Week.")
        return top_performing_listings

    except Exception as e:
        logging.error(f"Error calculating Top Performing Listings: {str(e)}")
        return pd.DataFrame()

# Function to calculate Most Popular Locations
def calculate_most_popular_locations(bookings, apartment_attr):
    try:
        bookings = bookings.merge(apartment_attr, left_on='apartment_id', right_on='id', how='left')
        bookings['booking_date'] = pd.to_datetime(bookings['booking_date'])
        bookings['week'] = bookings['booking_date'].dt.to_period('W').apply(lambda r: r.start_time)
        city_bookings = bookings.groupby(['week', 'cityname']).size().reset_index(name='total_bookings')
        return city_bookings.loc[city_bookings.groupby('week')['total_bookings'].idxmax()].reset_index(drop=True)
        logging.info("Calculated most popular locations")
    except Exception as e:
        logging.error(f"Error calculating Most popular locations: {str(e)}")
        return pd.DataFrame()  

# Function to calculate Total Bookings Per User
def calculate_total_bookings_per_user(bookings):
    try:
        confirmed = bookings[bookings['booking_status'] == 'confirmed']
        confirmed['booking_date'] = pd.to_datetime(confirmed['booking_date'])
        confirmed['week'] = confirmed['booking_date'].dt.to_period('W').apply(lambda r: r.start_time)
        return confirmed.groupby(['week', 'user_id']).size().reset_index(name='total_bookings')
        logging.info("Calculated total_bookings")
    except Exception as e:
        logging.error(f"Error calculating total_bookings per user: {str(e)}")
        return pd.DataFrame()

# Function to calculate Average Booking Duration
def calculate_avg_booking_duration(bookings):
    try:
        confirmed = bookings[bookings['booking_status'] == 'confirmed']
        confirmed['checkin_date'] = pd.to_datetime(confirmed['checkin_date'])
        confirmed['checkout_date'] = pd.to_datetime(confirmed['checkout_date'])
        confirmed['duration'] = (confirmed['checkout_date'] - confirmed['checkin_date']).dt.days
        confirmed['month'] = confirmed['checkin_date'].dt.to_period('M').apply(lambda r: r.start_time)
        return confirmed.groupby('month')['duration'].mean().reset_index(name='avg_booking_duration')
        logging.info("Calculated average booking duration")
    except Exception as e:
        logging.error(f"Error calculating average booking duration : {str(e)}")
        return pd.DataFrame()

# Function to calculate Repeat Customer Rate
def calculate_repeat_customer_rate(bookings):
    try:
        confirmed = bookings[bookings['booking_status'] == 'confirmed'].copy()
        confirmed['booking_date'] = pd.to_datetime(confirmed['booking_date'])
        confirmed = confirmed.sort_values(by=['user_id', 'booking_date'])
        confirmed = confirmed.groupby('user_id', group_keys=False).apply(
            lambda grp: grp.assign(rolling_count=grp.rolling('30D', on='booking_date')['booking_date'].count())
        )
        confirmed['repeat_user'] = confirmed['rolling_count'] > 1
        repeat_rate = confirmed.groupby(confirmed['booking_date'].dt.to_period('M')).agg(
            total_users=('user_id', 'nunique'),
            repeat_users=('repeat_user', 'sum')
        ).reset_index()
        repeat_rate['repeat_customer_rate'] = (repeat_rate['repeat_users'] / repeat_rate['total_users']) * 100
        return repeat_rate
        logging.info("Calculated conversion rate")
    except Exception as e:
        logging.error(f"Error calculating customer's repeat customer rate: {str(e)}")
        return pd.DataFrame()


def parallel_kpi_calculation(bookings, apartments, apartment_attr, user_viewings):
    """
    Parallel KPI calculation using concurrent.futures
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    kpi_functions = [
        (calculate_avg_listing_price_per_week, [apartments]),
        (calculate_occupancy_rate, [bookings, apartments]),
        (calculate_top_performing_listings_per_week, [bookings]),
        (calculate_most_popular_locations, [bookings, apartment_attr]),
        (calculate_total_bookings_per_user, [bookings]),
        (calculate_avg_booking_duration, [bookings]),
        (calculate_repeat_customer_rate, [bookings])
    ]
    
    results = {}
    with ThreadPoolExecutor(max_workers=min(7, len(kpi_functions))) as executor:
        future_to_kpi = {
            executor.submit(func, *args): func.__name__ 
            for func, args in kpi_functions
        }
        
        for future in as_completed(future_to_kpi):
            kpi_name = future_to_kpi[future]
            try:
                result = future.result()
                results[kpi_name] = result
            except Exception as e:
                logging.error(f"Error in {kpi_name}: {str(e)}")
    
    return results
        # Clean Column Names (Remove Whitespace & Convert from Bytes)
def clean_column_names(df):
    df.columns = [col.decode("utf-8") if isinstance(col, bytes) else col for col in df.columns]
    df.columns = df.columns.str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
    df.columns = [re.sub(r'[^a-zA-Z0-9_]', '', col) for col in df.columns]
    df.columns = [col[1:] if col else col for col in df.columns]  # Remove the first character of each column name
    return df
# Main execution
def main():

    # Fetch required data from Redshift and write to S3
    logging.info("Fetching data from Redshift and writing to S3...")
    apartments = read_from_redshift("SELECT id, price, listing_created_on, is_active FROM apartments;", CURATED_DB)
    write_to_s3(apartments, f"{S3_PREFIX}/apartments.csv")
    
    bookings = read_from_redshift("SELECT booking_id, user_id, apartment_id, booking_date, checkin_date, checkout_date, booking_status, total_price FROM bookings;", CURATED_DB)
    write_to_s3(bookings, f"{S3_PREFIX}/bookings.csv")
    
    apartment_attr = read_from_redshift("SELECT id, cityname FROM apartments_attributes;", CURATED_DB)
    write_to_s3(apartment_attr, f"{S3_PREFIX}/apartment_attr.csv")
    
    user_viewings = read_from_redshift("SELECT user_id, apartment_id, viewed_at FROM user_viewings;", CURATED_DB)
    write_to_s3(user_viewings, f"{S3_PREFIX}/user_viewings.csv")
    
    # Read data from S3
    logging.info("Reading data from S3...")
    apartments = read_from_s3(f"{S3_PREFIX}/apartments.csv")
    bookings = read_from_s3(f"{S3_PREFIX}/bookings.csv")
    apartment_attr = read_from_s3(f"{S3_PREFIX}/apartment_attr.csv")
    user_viewings = read_from_s3(f"{S3_PREFIX}/user_viewings.csv")
    
    # Apply cleaning function
    apartments = clean_column_names(apartments)
    bookings = clean_column_names(bookings)
    apartment_attr = clean_column_names(apartment_attr)
    user_viewings = clean_column_names(user_viewings)
    
    # Log column names in CloudWatch
    logging.info(f"Apartments Columns After Cleaning: {list(apartments.columns)}")
    logging.info(f"Cleaned Bookings Columns: {list(bookings.columns)}")
    logging.info(f"Cleaned Apartment Attributes Columns: {list(apartment_attr.columns)}")
    logging.info(f"Cleaned User Viewings Columns: {list(user_viewings.columns)}")
    
    # Parallel data loading and processing
    kpi_results = parallel_kpi_calculation(bookings, apartments, apartment_attr, user_viewings)
    
    # Bulk load KPIs to Redshift
    s3_staging_dir = f"{S3_PREFIX}/staging-area"
    kpi_table_mapping = {
        'calculate_avg_listing_price_per_week': 'kpis.avg_listing_price_per_week',
        'calculate_occupancy_rate': 'kpis.occupancy_rate_per_month',
        'calculate_top_performing_listings_per_week': 'kpis.top_performing_listings_per_week',
        'calculate_most_popular_locations': 'kpis.most_popular_locations',
        'calculate_total_bookings_per_user': 'kpis.total_bookings_per_user',
        'calculate_avg_booking_duration': 'kpis.avg_booking_duration',
        'calculate_repeat_customer_rate': 'kpis.repeat_customer_rate'
    }
    
    for func_name, df in kpi_results.items():
        if not df.empty:
            bulk_load_to_redshift(
                df, 
                kpi_table_mapping[func_name], 
                PRESENTATION_DB, 
                s3_staging_dir
            )

if __name__ == "__main__":
    main()