import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to redshift cluster
    """
    for query in copy_table_queries:
        try:
            print(f'Loading: {query}')
            cur.execute(query)
            conn.commit()
        except Exception as e:
            raise
            break


def insert_tables(cur, conn):
    """
    Insert staging tables to fact and dimension tables
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(query)
            raise
            break


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to cluster')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected')

    # execute staging tables

    print('Loading staging tables')
    load_staging_tables(cur, conn)

    # execute table inserts

    print('Inserting tables')
    insert_tables(cur, conn)

    conn.close()
    print('ETL pipeline completed')


if __name__ == "__main__":
    main()
