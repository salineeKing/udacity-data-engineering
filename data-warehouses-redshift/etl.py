import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
     """This function will execute sql commands which are defined in copy_table_queries variable to copy all data from S3 
        to the staging tables which are resided on AWS Redshift cluster

    Parameters: 
        cur: connection object from Psycopg's PostgreSQL database adapter 
        conn: cursor was created by the connection.cursor() method which allow Python code to execute PostgreSQL command in a database session

    Returns: 
        None

    """
        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
     """This function will execute sql commands which are defined in insert_table_queries variable to insert all records from 
        the staging tables to the fact & dimension tables
        Below is a summary of datasource from statging to fact & dimensions tables
        staging_events:
            - songplays
            - users
            - time
        staging_songs:
            - songplays
            - songs
            - artists

    Parameters: 
        cur: connection object from Psycopg's PostgreSQL database adapter 
        conn: cursor was created by the connection.cursor() method which allow Python code to execute PostgreSQL command in a database session

    Returns: 
        None

    """
        
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Copy data from S3(udacity-dend) to our staging tables
    load_staging_tables(cur, conn)
     # To let me know if the command is finished yet
    print('Finish copying data to staging tables!')
    
    # Insert data from our staging tables to our fact & dimension tables
    insert_tables(cur, conn)
     # To let me know if the command is finished yet
    print('Finish inserting data from staging tables to fact & dimension tables!')

    conn.close()
    


if __name__ == "__main__":
    main()