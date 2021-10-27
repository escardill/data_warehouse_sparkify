import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to redshift cluster
    """
    for query in copy_table_queries:
        print(f'Loading: {query}')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert staging tables to fact and dimension tables
    """
    for query in insert_table_queries:
        print(f'Inserting data: {query}')
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to cluster')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to cluster')

    # execute staging tables
    try:
        print('Loading staging tables')
        load_staging_tables(cur, conn)
        print('Loading SUCCESS')
    except Exception as e:
        print(e)
        print('Loading staging tables FAILED')

    # execute table inserts
    try:
        print('Inserting tables')
        insert_tables(cur, conn)
        print('SUCCESS')
    except Exception as e:
        print(e)
        print('Transformation from staging FAILED')

    conn.close()
    print('ETL pipeline completed')


if __name__ == "__main__":
    main()
