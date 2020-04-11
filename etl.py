import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, dq_queries


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


def run_data_quality_checks(cur, conn):
    """Run data quality checks

    Parameters
    ----------
    cur : Redshift database cursor
    conn: Redshift database connection
    """
    for table, query in dq_queries.items():
        cur.execute(query)
        query_results = cur.fetchone()
        print("Table {} has {:,} records.".format(table, query_results[0]))


def connect(config):
    """Make database connection"""
    connection = None
    try:
        # connect to the PostgreSQL server
        print('Making connection to redshift cluster...')
        connection = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

        # create a cursor
        cursor = connection.cursor()

        return cursor, connection
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if connection is not None:
            connection.close()
            print('Database connection closed.')


def main():
    conn = None
    try:
        # Load configuration data
        print('Loading configuration data...')
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        # Connect to Redshift cluster
        cur, conn = connect(config)

        # Load data to staging tables
        print('Staging data...')
        load_staging_tables(cur, conn)
        # Insert data to analytics tables
        print('Inserting records to final tables...')
        insert_tables(cur, conn)
        # Run data quality checks
        print('Running data quality checks...')
        run_data_quality_checks(cur, conn)
    except Exception as e:
        print("An error occurred: ", e)
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    finally:
        # Close database connection
        if conn is not None:
            print('Closing database connection...')
            conn.close()


if __name__ == "__main__":
    main()