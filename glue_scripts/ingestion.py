import boto3
import pandas as pd
import pymysql
import redshift_connector
import logging
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parse arguments from Glue job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'rds_host',
    'rds_port',
    'rds_user',
    'rds_password',
    'rds_database',
    'rds_tables',  # Comma-separated tables
    's3_bucket_name',
    's3_prefix',
    'redshift_host',
    'redshift_port',
    'redshift_user',
    'redshift_password',
    'redshift_raw_db',
    'iam_role'  # Redshift IAM Role ARN
])

# Validate required parameters
required_params = [
    'JOB_NAME', 'rds_host', 'rds_port', 'rds_user', 'rds_password', 'rds_database',
    'rds_tables', 's3_bucket_name', 's3_prefix', 'redshift_host', 'redshift_port',
    'redshift_user', 'redshift_password', 'redshift_raw_db', 'iam_role'
]
for param in required_params:
    if param not in args:
        logger.error(f"Missing required parameter: {param}")
        sys.exit(1)

# Log job arguments
logger.info(f"Glue job starting with parameters: {args}")

# Functions
def export_rds_table_to_s3(rds_params, table_name, s3_bucket, s3_prefix):
    try:
        logger.info(f"Connecting to RDS host: {rds_params['rds_host']}")
        conn = pymysql.connect(
            host=rds_params['rds_host'],
            port=int(rds_params['rds_port']),
            user=rds_params['rds_user'],
            password=rds_params['rds_password'],
            database=rds_params['rds_database']
        )
        query = f'SELECT * FROM {table_name}'
        df = pd.read_sql(query, conn)
        conn.close()

        # now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        s3_key = f'{s3_prefix}/{table_name}.csv'
        s3_path = f's3://{s3_bucket}/{s3_key}'

        # Save locally then upload to S3
        local_file = f'/tmp/{table_name}.csv'
        df.to_csv(local_file, index=False)

        s3 = boto3.client('s3')
        s3.upload_file(local_file, s3_bucket, s3_key)
        logger.info(f"Exported {table_name} to {s3_path}")
    except Exception as e:
        logger.error(f"Error exporting {table_name}: {e}", exc_info=True)
        raise e


def load_to_redshift_raw(redshift_params, s3_bucket, s3_prefix, table_name):
    try:
        logger.info(f"Connecting to Redshift host: {redshift_params['host']}")
        conn = redshift_connector.connect(
            host=redshift_params['host'],
            port=int(redshift_params['port']),
            database=redshift_params['database'],
            user=redshift_params['user'],
            password=redshift_params['password']
        )
        cursor = conn.cursor()

        s3_path = f's3://{s3_bucket}/{s3_prefix}/{table_name}.csv'
        copy_sql = f'''
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE '{redshift_params['iam_role']}'
            CSV
            IGNOREHEADER 1;
        '''
        logger.debug(f"Executing Redshift COPY command: {copy_sql}")
        cursor.execute(copy_sql)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Loaded data from {s3_path} into Redshift table {table_name}")
    except Exception as e:
        logger.error(f"Error loading {table_name} into Redshift: {e}", exc_info=True)
        raise e


def main():
    rds_params = {
        'rds_host': args['rds_host'],
        'rds_port': args['rds_port'],
        'rds_user': args['rds_user'],
        'rds_password': args['rds_password'],
        'rds_database': args['rds_database']
    }

    redshift_params = {
        'host': args['redshift_host'],
        'port': args['redshift_port'],
        'database': args['redshift_raw_db'],
        'user': args['redshift_user'],
        'password': args['redshift_password'],
        'iam_role': args['iam_role']
    }

    s3_bucket = args['s3_bucket_name']
    s3_prefix = args['s3_prefix']

    tables = args['rds_tables'].split(',')

    for table in tables:
        table = table.strip()
        logger.info(f"Processing table: {table}")
        export_rds_table_to_s3(rds_params, table, s3_bucket, s3_prefix)
        load_to_redshift_raw(redshift_params, s3_bucket, s3_prefix, table)


if __name__ == '__main__':
    main()