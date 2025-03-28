import boto3
import pandas as pd
import redshift_connector
import logging
import sys
from awsglue.utils import getResolvedOptions

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parse arguments from Glue job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'redshift_host',
    'redshift_port',
    'redshift_user',
    'redshift_password',
    'redshift_raw_db',
    'redshift_curated_db',
    'iam_role',
    's3_bucket',
    'tables' 
])

# Validate required parameters
required_params = [
    'JOB_NAME', 'redshift_host', 'redshift_port', 'redshift_user', 'redshift_password',
    'redshift_raw_db', 'redshift_curated_db', 'iam_role', 's3_bucket', 'tables'
]
for param in required_params:
    if param not in args:
        logger.error(f"Missing required parameter: {param}")
        sys.exit(1)

# Log job arguments
logger.info(f"Glue job starting with parameters: {args}")

# Function to connect to the RAW (landing) Redshift database
def get_raw_redshift_connection():
    try:
        logger.info(f"Connecting to RAW Redshift database: {args['redshift_raw_db']}")
        conn = redshift_connector.connect(
            host=args['redshift_host'],
            port=int(args['redshift_port']),
            database=args['redshift_raw_db'],
            user=args['redshift_user'],
            password=args['redshift_password']
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to RAW Redshift: {e}", exc_info=True)
        raise e

# Function to connect to the CURATED Redshift database
def get_curated_redshift_connection():
    try:
        logger.info(f"Connecting to CURATED Redshift database: {args['redshift_curated_db']}")
        conn = redshift_connector.connect(
            host=args['redshift_host'],
            port=int(args['redshift_port']),
            database=args['redshift_curated_db'],
            user=args['redshift_user'],
            password=args['redshift_password']
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to CURATED Redshift: {e}", exc_info=True)
        raise e

# Extract, Transform, Load (ETL) Function
def transform_and_load(table_name):
    try:
        # Step 1: Extract data from RAW (landing) table
        raw_conn = get_raw_redshift_connection()
        raw_cursor = raw_conn.cursor()
        
        logger.info(f"Extracting data from RAW table: {table_name}")
        extract_sql = f"SELECT * FROM landing.public.{table_name}"
        df = pd.read_sql(extract_sql, raw_conn)
        
        # Close RAW connection after extracting data
        raw_cursor.close()
        raw_conn.close()

        # Step 2: Apply Transformations
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Example transformation: Converting price column to float if exists
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Example transformation: Standardizing column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        
        # Example transformation: Removing duplicates
        df.drop_duplicates(inplace=True)

        # Step 3: Save transformed data to S3
        s3 = boto3.client('s3')
        s3_bucket = args['s3_bucket']
        s3_key = f'curated/{table_name}.csv'
        local_file = f'/tmp/{table_name}.csv'

        df.to_csv(local_file, index=False)
        s3.upload_file(local_file, s3_bucket, s3_key)
        logger.info(f"Uploaded transformed data for {table_name} to S3: s3://{s3_bucket}/{s3_key}")

        # Step 4: Load transformed data into CURATED Redshift schema
        curated_conn = get_curated_redshift_connection()
        curated_cursor = curated_conn.cursor()

        copy_sql = f"""
            COPY curated.public.{table_name}
            FROM 's3://{s3_bucket}/{s3_key}'
            IAM_ROLE '{args['iam_role']}'
            CSV
            IGNOREHEADER 1;
        """
        curated_cursor.execute(copy_sql)
        curated_conn.commit()

        # Close CURATED connection after loading data
        curated_cursor.close()
        curated_conn.close()

        logger.info(f"Successfully transformed and loaded {table_name} into CURATED schema")
    except Exception as e:
        logger.error(f"Error processing {table_name}: {e}", exc_info=True)
        raise e

# Process tables
def main():
    tables = args['tables'].split(',')
    for table in tables:
        transform_and_load(table.strip())

if __name__ == '__main__':
    main()
    logger.info("Glue job completed successfully.")