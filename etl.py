import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function loads raw data from AWS S3 to AWS Redshift Database

    Parameters
    ----------
    cur : Redshift database cursor
    conn: Redshift database connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data from Redshift staging tables into Redshift Analytic tables

    Parameters
    ----------
    cur : Redshift database cursor
    conn: Redshift database connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Load configuration data
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Load data to staging tables
    load_staging_tables(cur, conn)
    # Insert data to analytics tables
    insert_tables(cur, conn)

    # Close database connection
    conn.close()


if __name__ == "__main__":
    main()