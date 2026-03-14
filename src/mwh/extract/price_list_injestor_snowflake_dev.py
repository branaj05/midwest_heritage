"""
Midwest Heritage — Price List ingester (with parse_headers + schema integration)

This script ingests the Price_List sheet, uses the user-defined parse_headers.py
functions for header parsing, and inserts into the schema defined in price_list_schema.sql.
"""
from __future__ import annotations
from numpy import rint
from tqdm import tqdm
import pandas as pd
import os
import importlib
from sqlalchemy import create_engine, engine, text, MetaData, Table
from urllib.parse import quote_plus
from mwh.extract.parse_headers import parse_header_fields
from mwh.utils.utils import go_up_dirs, read_config, col_to_index, load_sql
from datetime import datetime

NATURAL_KEY = ["category1_id", "category2_id", "category3_id", "quote_date", "price_source_id", "description"]
PRICE_LIST_MERGE = load_sql("price_list_upsert.sql")

#%% Local Utils
def test_connection(engine):
    with engine.connect() as conn:
        result = conn.execute(text('select current_version()')).fetchone()
        print(f"Connected to Snowflake version: {result[0]}")
#%% Clean Data Helpers
def squash_rows(df):
    price_cols = df.columns[10:].tolist()  # adjust index to where prices start
    df = df[df[price_cols].apply(pd.to_numeric, errors='coerce').notna().any(axis=1)]
    return df

def label_dupes(df, key_cols = ['Lvl 1 Category', 'Lvl 2 Category', 'Lvl 3 Category', 'Description']):
    # Fill NaN descriptions with a placeholder to ensure they are included in dupe detection
    df['Description']=df['Description'].fillna('Unknown')
    # Pull Dupes
    dupes = df[key_cols][df[key_cols].duplicated(keep=False)]
    # Handle Dupes
    if dupes.size > 0:
        # Save dupes to a CSV for logging/debugging purposes
        path = os.path.join(go_up_dirs(__file__, 2), "logs", 'dupes.csv')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        dupes.to_csv(path)

        # HOTFIX for dupes: append a cumulative count to the Description to make them unique
        cumcount = df.groupby(key_cols, dropna=False).cumcount()
        df['Description'] = df['Description'].where(
            cumcount == 0,
            df['Description'] + '_' + cumcount.astype(str)
        )

        # Re-check for dupes after the hotfix, dump them, and raise an error
        dupes = df[key_cols][df[key_cols].duplicated(keep=False)]
        if dupes.size > 0:
            path = os.path.join(go_up_dirs(__file__, 2), "eraseme_data_dump", 'dupes_post.csv')
            os.makedirs(os.path.dirname(path), exist_ok=True)
            dupes.to_csv(os.path.join(path, 'dupes.csv'))
            raise ValueError(f"Duplicate rows found based on key columns {key_cols}. Check the 'dupes_post.csv' output for details.")
    return df
def clean_data(df):
    df = squash_rows(df)
    df = label_dupes(df)
    return df

def reflect_tables(engine, schema="price_list"):
    # # Define metadata object for the given schema
    md = MetaData()
    # call reflect to load table definitions from the database
    with engine.connect() as conn:
        md.reflect(bind=conn, schema=schema)
    return md


# # TODO: Maybe generalize this to get_or_create_by_cols(conn, table, col_val_dict, id_col)
# def get_or_create(conn, table: Table, name_value: str, name_col: str, id_col: str):
#     """
#     Helper: get_or_create

#     Ensures a consistent foreign-key reference for lookup tables
#     (price_categories, quote_type, price_source, unit_of_measure).

#     Workflow:
#         - Accepts a connection, a reflected SQLAlchemy Table, a string value,
#         the column name to match on (e.g., 'name'), and the table’s ID column.
#         - Attempts to INSERT the given value into the lookup table.
#         - If the value already exists, uses PostgreSQL's ON CONFLICT to do a no-op update.
#         - Always returns the primary key ID (via RETURNING), so the calling code
#         can use it when inserting into price_list or related tables.

#     Why:
#         This prevents duplicate rows for things like vendors or units of measure,
#         while also saving you from writing separate SELECT-then-INSERT logic.
#         Effectively: "insert if new, else fetch the existing ID."
#     """
#     if not name_value:
#         return None
#     merge_stmt = text(f"""
#         MERGE INTO {table.fullname} AS target
#         USING (SELECT :name AS {name_col}) AS source
#         ON target.{name_col} = source.{name_col}
#         WHEN NOT MATCHED THEN
#             INSERT ({name_col}) VALUES (source.{name_col})
#         WHEN MATCHED THEN
#             UPDATE SET target.{name_col} = source.{name_col}
#     """)
#     conn.execute(merge_stmt, {"name": name_value})
#     id_stmt = text(f"SELECT {id_col} FROM {table.fullname} WHERE {name_col} = :name")
#     return int(conn.execute(id_stmt, {"name": name_value}).scalar())


# def get_or_create_category(conn, name_value: str, level_value: str) -> int | None:
#     """
#     Helper: get_or_create_category

#     Ensures a consistent foreign-key reference in price_list.price_categories
#     for a given (name, level) pair.

#     Args:
#         conn       : SQLAlchemy connection (inside a transaction block)
#         name_value : Category name (e.g., "Lumber", "Dimensional Lumber")
#         level_value: Category level (e.g., "1", "2", "3")

#     Returns:
#         The category_id (int) for the row, inserting if necessary.

#     Behavior:
#         • Attempts to INSERT (name, level).
#         • If a row with the same name already exists, does nothing destructive
#           but updates the level to the new value.
#         • Always returns the primary key id via RETURNING.
#     """
#     if not name_value:
#         return None

#     merge_stmt = text("""
#         MERGE INTO price_list.price_categories AS target
#         USING (SELECT :name AS name, :level AS level) AS source
#         ON target.name = source.name AND target.level = source.level
#         WHEN MATCHED THEN UPDATE SET target.level = source.level
#         WHEN NOT MATCHED THEN INSERT (name, level) VALUES (source.name, source.level)
#     """)
#     conn.execute(merge_stmt, {"name": name_value, "level": level_value})
#     id_stmt = text("SELECT category_id FROM price_list.price_categories WHERE name = :name AND level = :level")
#     return int(conn.execute(id_stmt, {"name": name_value, "level": level_value}).scalar())


# # def load_category_cache(conn):
# #     result = conn.execute(text("SELECT name, level, category_id FROM price_list.price_categories")).fetchall()
# #     return {(row.name, row.level): row.category_id for row in result}

# # def get_or_create_category_cached(conn, cache, name_value, level_value):
# #     if not name_value:
# #         return None
# #     key = (name_value, level_value)
# #     if key in cache:
# #         return cache[key]
# #     # Cache miss - insert and update cache
# #     merge_stmt = text("""
# #         MERGE INTO price_list.price_categories AS target
# #         USING (SELECT :name AS name, :level AS level) AS source
# #         ON target.name = source.name AND target.level = source.level
# #         WHEN NOT MATCHED THEN INSERT (name, level) VALUES (source.name, source.level)
# #         WHEN MATCHED THEN UPDATE SET target.level = source.level
# #     """)
# #     conn.execute(merge_stmt, {"name": name_value, "level": level_value})
# #     id_stmt = text("SELECT category_id FROM price_list.price_categories WHERE name = :name AND level = :level")
# #     category_id = int(conn.execute(id_stmt, {"name": name_value, "level": level_value}).scalar())
# #     cache[key] = category_id
# #     return category_id

#%% Staging Helpers
def load_lookup(conn, table, id_col, name_col):
    """Load entire lookup table into a dict: name -> id"""
    result = conn.execute(text(f"SELECT {id_col}, {name_col} FROM {table.fullname}")).fetchall()
    return {row[1]: row[0] for row in result}

def load_categories(conn):
    result = conn.execute(text("SELECT category_id, name, level FROM price_list.price_categories")).fetchall()
    return {(row[1], row[2]): row[0] for row in result}
# -----------------------------
# Main ingest function
# -----------------------------

def ingest_price_list(
    xlsx_path    : str,
    sheet_name   : str = "Price_List",
    engine_url   : str = "postgresql+psycopg2://user:pass@localhost:5432/midwest",
    schema       : str = "price_list",
    header_row   : int | None = None,
    index_cols   : int = 1,
    BATCH_SIZE   : int = 250,
):
    """
    Ingests the price list Excel file into the database schema defined by price_list_schema.sql.
    Uses parse_headers.parse_header_fields for parsing header metadata.
    """
    # Define the natural key (uniqueness definition for price_list)
    
    #############################################################################
    # Read Excel File and Parse for metadata 
    if isinstance(index_cols, str):
        index_cols = col_to_index(index_cols)
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    
    #   Infer header row if not given
    if header_row is None:
        for i in range(min(30, len(df))):
            # walk down rows until we find one with at least 2 non-nan values after index_cols
            non_nan_after_idx = df.iloc[i, index_cols:].notna().sum()
            if non_nan_after_idx >= 2:
                header_row = i
                break
    if header_row is None:
        raise ValueError("Could not infer header row.")

    #   Parse headers
    header_metas = {}
    for col in range(index_cols, df.shape[1]):
        hv = df.iat[header_row, col]
        if hv and isinstance(hv, str):
            header_metas[col] = parse_header_fields(hv)

    #   Candidate rows
    body = df.iloc[header_row + 1 :].reset_index(drop=True)
    body.columns = df.iloc[header_row, :]
    # Clean body: drop rows with no price data and label duplicate rows (HOTFIX, data quality issue that needs fixed at the source)
    body = squash_rows(body)
    body = label_dupes(body)
    
    #############################################################################
    # Connect to DB and reflect tables
    engine = create_engine(engine_url)
    test_connection(engine)
    #   Reflect tables and load lookups into cache dicts (keep for now)
    md = reflect_tables(engine, schema=schema)
    # price_list = md.tables.get(f"{schema}.price_list")
    # categories = md.tables.get(f"{schema}.price_categories")
    # quote_types = md.tables.get(f"{schema}.quote_type")
    price_sources = md.tables.get(f"{schema}.price_source")
    uoms = md.tables.get(f"{schema}.unit_of_measure")
    with engine.connect() as conn:
        # One Trip to load lookups
        with conn.begin():
            category_cache = load_categories(conn)
            source_cache = load_lookup(conn, price_sources, "price_source_id", "name")
            uom_cache = load_lookup(conn, uoms, "unit_of_measure_id", "name")
        #############################################################################
        # STAGE payloads locally
        payloads = []
        new_categories = {}  # (name, level) -> placeholder
        new_sources = set()
        new_uoms = set()
        for r_idx, row in tqdm(body.iterrows(), total=body.shape[0], desc = "Staging Data..."):
            lvl1 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
            lvl2 = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None
            lvl3 = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None

            # Track new categories for bulk insert later
            for name, level in [(lvl1, "1"), (lvl2, "2"), (lvl3, "3")]:
                if name and (name, level) not in category_cache:
                    new_categories[(name, level)] = None  # ID to be resolved after insert
            
            valid_cols = [c for c in header_metas.keys() if pd.notna(row.iloc[c])]
            for c_idx in valid_cols:
                val = row.iloc[c_idx]
                hm = header_metas[c_idx]
                event_date = hm.get("event_date")
                if event_date is None:
                    continue
                if isinstance(event_date, str):
                    event_date = datetime.strptime(event_date, "%m/%d/%y").date()

                source = hm.get("event_source")
                uom = hm.get("event_unit_measure")
                if source and source not in source_cache:
                    new_sources.add(source)
                if uom and uom not in uom_cache:
                    new_uoms.add(uom)

                payloads.append({
                    "lvl1": lvl1, "lvl2": lvl2, "lvl3": lvl3,  # resolve IDs after lookup inserts
                    "description": str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else None,
                    "source": source,
                    "uom": uom,
                    "quote_date": event_date,
                    "dim_thickness": str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else None,
                    "dim_width": str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else None,
                    "dim_length": str(row.iloc[7]).strip() if pd.notna(row.iloc[7]) else None,
                    "price_value": float(val) if isinstance(val, (int, float)) else None,
                    "notes": None,
                })
        #############################################################################
        # 'Bulk' insert any new lookup values
        with conn.begin():
            if new_categories:
                conn.execute(text("INSERT INTO price_list.price_categories (name, level) VALUES (:name, :level)"),
                            [{"name": n, "level": l} for n, l in new_categories.keys()])
            if new_sources:
                conn.execute(text("INSERT INTO price_list.price_source (name) VALUES (:name)"),
                            [{"name": s} for s in new_sources])
            if new_uoms:
                conn.execute(text("INSERT INTO price_list.unit_of_measure (name) VALUES (:name)"),
                            [{"name": u} for u in new_uoms])
        
        # Reload caches with new IDs
        with conn.begin(): 
            category_cache = load_categories(conn)
            source_cache = load_lookup(conn, price_sources, "price_source_id", "name")
            uom_cache = load_lookup(conn, uoms, "unit_of_measure_id", "name")

        # Resolve FKs and build final payloads
        final_payloads = []
        for p in payloads:
            final_payloads.append({
                "category1_id": category_cache.get((p["lvl1"], "1")),
                "category2_id": category_cache.get((p["lvl2"], "2")),
                "category3_id": category_cache.get((p["lvl3"], "3")),
                "description": p["description"],
                "price_source_id": source_cache.get(p["source"]),
                "quote_date": p["quote_date"],
                "dim_thickness": p["dim_thickness"],
                "dim_width": p["dim_width"],
                "dim_length": p["dim_length"],
                "unit_of_measure_id": uom_cache.get(p["uom"]),
                "price_value": p["price_value"],
                "notes": p["notes"],
            })

    # Build dataframe from final payloads
    df = pd.DataFrame(final_payloads)
    # Get raw snowflake connection from SQLAlchemy engine
    # Bulk load into a temp staging table - one round trip
    if True:
        print(f"df shape: {df.shape}")
        print(df.head())
        df.to_sql(
            "price_list_stage",
            con=engine,
            schema="price_list",
            if_exists="replace",
            index=False
        )
        with engine.connect() as verify_conn:
            count = verify_conn.execute(text("SELECT COUNT(*) FROM price_list.price_list_stage")).scalar()
            print(f"Staging table row count after to_sql: {count}")
    # if True:
    #     from snowflake.connector.pandas_tools import write_pandas
    #     write_pandas(
    #         raw_conn,
    #         df,
    #         "price_list_stage",
    #         schema="price_list",
    #         auto_create_table=True,
    #         overwrite=True  # ← truncates staging table before each load
    #     )
    # Single MERGE from staging into target
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("""
                MERGE INTO price_list.price_list AS target
                USING price_list.price_list_stage AS source
                ON  target.category1_id = source.category1_id
                AND target.category2_id = source.category2_id
                AND target.category3_id = source.category3_id
                AND target.quote_date = source.quote_date
                AND target.description = source.description
                AND EQUAL_NULL(target.price_source_id, source.price_source_id)
                WHEN MATCHED THEN UPDATE SET
                    target.dim_thickness = source.dim_thickness,
                    target.dim_width = source.dim_width,
                    target.dim_length = source.dim_length,
                    target.unit_of_measure_id = source.unit_of_measure_id,
                    target.price_value = source.price_value,
                    target.notes = source.notes
                WHEN NOT MATCHED THEN INSERT (
                    category1_id, category2_id, category3_id, description,
                    price_source_id, quote_date, dim_thickness, dim_width,
                    dim_length, unit_of_measure_id, price_value, notes
                ) VALUES (
                    source.category1_id, source.category2_id, source.category3_id, source.description,
                    source.price_source_id, source.quote_date, source.dim_thickness, source.dim_width,
                    source.dim_length, source.unit_of_measure_id, source.price_value, source.notes
                )
            """))
    with engine.connect() as verify_conn:
        count = verify_conn.execute(text("SELECT COUNT(*) FROM price_list.price_list")).scalar()
        print(f"Target table row count after MERGE: {count}")

if __name__ == "__main__":
    import os

    # --- File input ---
    data_directory = r"C:\Users\austi\OneDrive\Documents\Contracts\Midwest Heritage\Data\working"
    price_list_name = "25.08.08_jobname_EstSheet_v25.08.04 (LamarTest).xlsx"
    path = os.path.join(data_directory, price_list_name)

    # --- Database connection from .ini ---
    # For Snowflake, we have a separate config file with the Snowflake connection details
    snowflake_ini = os.path.join(go_up_dirs(__file__, 2), "configs", "snowflake.ini")
    params = read_config(snowflake_ini, section="snowflake")

    # Build SQLAlchemy connection URL
    ENGINE = (
        f"snowflake://{params['user']}:{quote_plus(params['password'])}"
        f"@{params['account']}"
        f"/{params['database']}"
        f"?warehouse={params['warehouse']}&role={params['role']}"
    )

    # Run the ingest
    inserted = ingest_price_list(
        path, 
        sheet_name="Price_List", 
        engine_url=ENGINE, 
        header_row=18, 
        index_cols='ab')
    print(f"Successfully ingested {inserted} rows.")
    # TODO: ADD the rest of the fields for the price list table and get this working. 