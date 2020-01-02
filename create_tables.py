import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """This function drops AWS Redshift Database tables

    Parameters
    ----------
    cur : Redshift database cursor
    conn: Redshift database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function creates required database tables in AWS Redshift

    Parameters
    ----------
    cur : Redshift database cursor
    conn: Redshift database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Load configuration data
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # drop database tables in they exists
    drop_tables(cur, conn)
    # create database tables
    create_tables(cur, conn)

    # close database connection
    conn.close()


if __name__ == "__main__":
    main()