#%% IMPORTS
import pyodbc
import pandas as pd

#%% Connect to SQL Server

# Make sure you have the ODBC Driver 17 for SQL Server installed
# define connection stringe
connection_string = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=DESKTOP-MDPITVH\AUSTI;"
    r"Database=midwest_heritage;" 
    r"Trusted_Connection=yes;"
)
# Connect to SQL Server
connection = pyodbc.connect(connection_string)

#%% Fetch data from a table
# Example query to fetch data from a table
df = pd.read_sql("SELECT * FROM dbo.YourTableName", connection)
print(df.head())



#%% Junk

f= 0
for j in range(0, n):
    f = f+w[j] * x[j]

f = f+b

________VS________
f = np.dot(w, x) + b
