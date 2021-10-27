import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all the tables in drop_table_queries
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all the tables in create_table_queries
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to cluster')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to cluster')

    print('Dropping existint tables')
    try:
        drop_tables(cur, conn)
    except Exception as e:
        print(f"Tables droping failed. Exception: {e}")

    print("Creating fact, dimensions and staging tables")

    try:
        create_tables(cur, conn)
    except Exception as e:
        print(f"Tables creation failed. Exception: {e}")

    conn.close()
    print("Tables creation process ended")


if __name__ == "__main__":
    main()
