import boto3
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, query_params):
    """Pulls data from S3 buckets and inserts the data into the staging tables."""
    for query, params in list(zip(copy_table_queries, query_params)):
        cur.execute(query.format(*params))
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data from the staging tables into the data analysis tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def get_s3_client(key, secret):
    """Gets an S3 client."""
    return boto3.resource(
        's3',
        region_name="us-west-2",
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )


def main():
    """
    Connects to a Redshift cluster and pulls user event data and song data from S3 buckets into
    the staging tables. Then inserts data from the staging tables into the data analysis tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    s3 = get_s3_client(config.get("AWS", "KEY"), config.get("AWS", "SECRET"))
    
    role_arn = config.get("IAM_ROLE", "ARN")
    staging_query_params = [
        (config.get("S3", "LOG_DATA"), role_arn, config.get("S3", "LOG_JSONPATH")),
        (config.get("S3", "SONG_DATA"), role_arn)
    ]
    print("--- Loading data into staging tables.")
    load_staging_tables(cur, conn, staging_query_params)
    print("--- Inserting data into analysis tables.")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()