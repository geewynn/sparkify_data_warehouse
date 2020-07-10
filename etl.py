import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Load data into the staging tables
    Arguments:
    cur (psycopg2.cursor): indicates the psycopg2 cursor
    conn (psycopg2.connection): indicates the postgres connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Insert the data from the staging tables to 
    the analytical tables
    Arguments:
    cur (psycopg2.cursor): indicates the psycopg2 cursor
    conn (psycopg2.connection): indicates the postgres connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()