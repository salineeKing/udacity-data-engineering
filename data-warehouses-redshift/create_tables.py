import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """This function will execute sql commands which are defined in drop_table_queries variable to drop all tables  

    Parameters: 
        cur: connection object from Psycopg's PostgreSQL database adapter 
        conn: cursor was created by the connection.cursor() method which allow Python code to execute PostgreSQL command in a database session

    Returns: 
        None

    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
     """This function will execute sql commands which are defined in create_table_queries variable to create all tables  

    Parameters: 
        cur: connection object from Psycopg's PostgreSQL database adapter 
        conn: cursor was created by the connection.cursor() method which allow Python code to execute PostgreSQL command in a database session

    Returns: 
        None

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # First, drop all tables if exists
    drop_tables(cur, conn)
    # Then, create all tables
    create_tables(cur, conn)
    # To let me know if the command is finished yet
    print('Finish creating all tables!')

    conn.close()


if __name__ == "__main__":
    main()