import os
import pandas as pd
import numpy as np
from configparser import ConfigParser

def load_sql(filename, directory="snowflake"):
    path = os.path.join(go_up_dirs(__file__, 2), "sql", directory, filename)
    with open(path, 'r') as f:
        return f.read()
    
def go_up_dirs(path, n):
    """Go up n directories from path."""
    for _ in range(n):
        path = os.path.dirname(path)
    return path

def read_config(filename='database.ini', section='postgresql'):
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

def col_to_index(col: str) -> int:
    """Convert Excel column letter to zero-based index."""
    col = col.upper()
    result = 0
    for c in col:
        result = result * 26 + (ord(c) - ord('A') + 1)
    return result - 1   # make it 0-based (A=0)