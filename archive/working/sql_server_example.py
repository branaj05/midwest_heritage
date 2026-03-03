#%%
import pandas as pd
from sqlalchemy import create_engine, String, Integer, Float, DateTime, Boolean
from sqlalchemy.dialects.postgresql import VARCHAR
from sqlalchemy.engine import URL

#%%
# 0) Init
fname = r"C:\Users\austi\OneDrive\Documents\Contracts\Midwest Heritage\Data\working\import_table.xlsx"
#%% Read and Format Excel
# 1) Read Excel
df = pd.read_excel(
    fname,
    sheet_name="Database",
    header=0,            # zero-based row index; 2 means the 3rd row has column names
    usecols="A:BK",     # Excel range for columns if you need it; can also pass list of ints/names
    dtype=None          # let pandas infer, but we’ll override where important
)

# optional cleanup
df.columns = (
    df.columns
      .str.strip()
      .str.lower()
      .str.replace(r"[^0-9a-zA-Z]+", "_", regex=True)
      .str.strip("_")
)
#%%
# 2) Create an engine (Postgres example)
engine = create_engine("postgresql+psycopg2://user:pass@host:5432/dbname")
engine = create_engine("postgresql+psycopg2://DESKTOP-MVR2B00/austi:pass@host:")

# 3) Map dtypes explicitly for critical columns
dtype_map = {
    "id": Integer(),
    "name": VARCHAR(120),
    "amount": Float(),
    "created_at": DateTime(),
    "is_active": Boolean()
    # others fall back to SQLAlchemy inference if omitted, but being explicit is safer
}

# 4) Load to a staging table
df.to_sql(
    "my_table_stg",
    engine,
    if_exists="replace",   # for idempotent runs; use "append" in production
    index=False,
    dtype=dtype_map,
    chunksize=10000
)
