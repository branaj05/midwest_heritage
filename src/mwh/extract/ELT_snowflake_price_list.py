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

NATURAL_KEY = ["category1_id", "category2_id", "category3_id", "quote_date", "price_source_id", "description", "unit_of_measure_id"]
PRICE_LIST_MERGE = load_sql("price_list_merge.sql")

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

        # Invert caches for readable dupe output (id -> name)
        inv_category = {v: k[0] for k, v in category_cache.items()}
        inv_source   = {v: k for k, v in source_cache.items()}
        inv_uom      = {v: k for k, v in uom_cache.items()}

    # Build dataframe from final payloads
    df = pd.DataFrame(final_payloads)

    # Enforce natural key uniqueness before staging (Snowflake does not enforce UNIQUE constraints)
    dupes_mask = df.duplicated(subset=NATURAL_KEY, keep=False)
    if dupes_mask.any():
        dupe_path = os.path.join(go_up_dirs(__file__, 2), "logs", "dupes_long.csv")
        os.makedirs(os.path.dirname(dupe_path), exist_ok=True)
        dupes = df[dupes_mask].copy()
        dupes.insert(0, "category1",    dupes["category1_id"].map(inv_category))
        dupes.insert(1, "category2",    dupes["category2_id"].map(inv_category))
        dupes.insert(2, "category3",    dupes["category3_id"].map(inv_category))
        dupes.insert(5+3, "price_source", dupes["price_source_id"].map(inv_source))
        dupes.insert(7+6, "uom",          dupes["unit_of_measure_id"].map(inv_uom))
        dupes.sort_values(NATURAL_KEY).to_csv(dupe_path, index=False)
        raise ValueError(
            f"{dupes_mask.sum()} rows share a duplicate natural key. "
            f"Inspect: {dupe_path}"
        )

    # Bulk load into a temp staging table - one round trip
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
    # If using this block, define raw_conn; Get raw snowflake connection from SQLAlchemy engine
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
            conn.execute(text(PRICE_LIST_MERGE))
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
    ingest_price_list(
        path, 
        sheet_name="Price_List", 
        engine_url=ENGINE, 
        header_row=18, 
        index_cols='ab')
