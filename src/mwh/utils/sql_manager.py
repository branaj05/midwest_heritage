from abc import ABC, abstractmethod
import psycopg2
import snowflake.connector
import os
from mwh.utils.utils import go_up_dirs, read_config

class manager(ABC):
    def __init__(self):
        # establish path to .ini file and read in connectoin parameters
        self.config_dir = os.path.join(go_up_dirs(__file__, 2), 'config')
        self.sql_dir = os.path.join(go_up_dirs(__file__, 2), 'sql')
        # initialize connection and cursor to None
        self.conn = None
        self.cursor = None

    @property
    @abstractmethod
    def _connector(self):
        """Subclasses return the connection callable here."""
        pass
    @property
    @abstractmethod
    def config(self):
        pass

    def connect(self):
        """ Connect to the database server """
        if self.conn is not None:
            return self.conn
        else:
            # connect to the PostgreSQL server
            print('Connecting to the database...')
            self.conn = self._connector(**self.config)
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

    def _object_exists(self, table_name):
        self.cur.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table_name.lower()}'
        """)
        return self.cur.fetchone()[0] > 0

class postgres(manager):
    # def __init__(self):

    @property
    def _connector(self):
        import psycopg2
        return psycopg2.connect
    @property
    def config(self):
        return read_config(filename=self.config_file, section='postgresql')
        
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

class snowflake():
    # def __init__(self):
        # establish path to .ini file and read in connectoin parameters
    @property
    def _connector(self):
        import snowflake.connector
        return snowflake.connector.connect
    @property
    def config(self):
        """
        .. /inputs/snowflake.ini
        [snowflake]
        user="username",
        password="password",e
        account="account_name",
        warehouse="warehouse_name",
        role="role_name"
        """
        return read_config(filename=self.config_file, section='snowflake')
    
    def upload_schema(self, schema_sql_path):
        """ Upload and execute a SQL schema from a file"""
        if self.conn is None:
            self.connect()  
        with open(schema_sql_path, 'r') as file:
            schema_sql = file.read()
        
        # Split into multiple statmeents
        statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
        executed = []

        try:
            for stmt in statements:
                self.cur.execute(stmt)
                executed.append(stmt)
            print("Schema uploaded successfully.")
        except Exception as e:
            print(f"Upload Schema Failed. Be sure to use only on fresh environments. Error: {e}")
            # print(f"Failed. Attempting to roll back {len(executed)} statements...")
            # self._compensate(executed)
            raise e
        

    
    # def _compensate(self, executed_statements):
    #     """ Attempt roll back by dropping created objects"""
    #     for stmt in reversed(executed_statements):
    #         upper = stmt.upper()
    #         if 'CREATE TABLE' in upper:
    #             table = self._extract_object_name(stmt)
    #             try:
    #                 self.cur.execute(f"DROP TABLE IF EXISTS {table}")
    #                 print(f"Dropped Table {table}")
    #             except Exception as e:
    #                 print(f"Could not drop {table}: {e}")
    #         elif 'CREATE SCHEMA' in upper:
    #             schema = self._extract_object_name(stmt)
    #             try:
    #                 self.cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    #                 print(f"Dropped Schema {schema}")
    #             except Exception as e:
    #                 print(f"Could not drop {schema}: {e}")

    # def _extract_object_name(self, stmt):
    #     """Naive extraction - gets the word after CREATE TABLE/SCHEMA/VIEW."""
    #     tokens = stmt.upper().split()
    #     for i, token in enumerate(tokens):
    #         if token in ('TABLE', 'SCHEMA', 'VIEW') and i + 1 < len(tokens):
    #             return stmt.split()[i + 1]  # preserve original case
        


