import configparser
import psycopg2
from sql_queries import copy_table_queries


def load_staging_tables(cur, conn):
    """
    Function to load staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


# def insert_tables(cur, conn):
#     """
#     Function to insert data from staging table into facts and dimension tables
#     """
#     for query in insert_table_queries:
#         cur.execute(query)
#         conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('api.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    # insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
