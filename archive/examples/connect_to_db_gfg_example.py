#%% Sources: 
# GeeksForGeeks | https://www.geeksforgeeks.org/postgresql/postgresql-connecting-to-the-database-using-python/
#%% Basic Connection to PostgreSQL database using psycopg2
import psycopg2

# Connect to your postgres DB
conn = psycopg2.connect(
    dbname = "midwest_heritage",
    user="postgres",
    password="password",
    host="localhost",
)

# %% Connection to PostgreSQL database using a '.ini' file
""""
database.ini file content example:

[postgresql]
host=localhost
database=midwest_heritage
user=postgres
password=password

"""
from configparser import ConfigParser
import psycopg2

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return db

def connect():

    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        print("Connection Successful")
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')   

if __name__ == '__main__':
    connect()