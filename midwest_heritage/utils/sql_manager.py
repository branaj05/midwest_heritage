import psycopg2
import os
from midwest_heritage.utils.utils import go_up_dirs, read_config

class manager:
    def __init__(self):
        """ Initialize the database manager by reading connection parameters from a .ini file """
        # establish path to .ini file and read in connectoin parameters
        db_ini = os.path.join(go_up_dirs(__file__, 2), 'inputs', 'connect2database.ini')
        self.db_config = read_config(filename=db_ini, section='postgresql')
        # initialize connection and cursor to None
        self.conn = None
        self.cursor = None

    def connect(self):
        """ Connect to the PostgreSQL database server """
        if self.conn is not None:
            return self.conn
        else:
            # establish connection parameters
            params = self.db_config

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**params)

            # create a cursor
            self.cur = self.conn.cursor()
            print("Connection Successful")

            return self.conn
            
    def close(self):
        """ Close the database connection """
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        if self.cur is not None:
            self.cur.close()
            self.cur = None
        print("Database connection closed.")
            
        
    # NOTE: This is a very basic execute_query function; in practice, you might want to add error handling, logging, etc.
    def execute_query(self, query):
        """ Execute a SQL query and return the results """
        if self.conn is None:
            self.connect()
        self.cur.execute(query)
        self.cur.commit()
        return self.cur.fetchall()
    
    def upload_schema(self, schema_sql_path):
        """ Upload and execute a SQL schema from a file"""
        if self.conn is None:
            self.connect()
        with open(schema_sql_path, 'r') as file:
            schema_sql = file.read()
        try: 
            self.cur.execute(schema_sql)
            self.conn.commit()
            print("Schema uploaded successfully.")
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            self.close()
        


        
        



