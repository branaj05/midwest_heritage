"""
Midwest Heritage — Price List ingester (with parse_headers + schema integration)

This script ingests the Price_List sheet, uses the user-defined parse_headers.py
functions for header parsing, and inserts into the schema defined in price_list_schema.sql.
"""
from __future__ import annotations
from tqdm import tqdm
import pandas as pd
import os
import importlib
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from mwh.parse_headers import parse_header_fields
from mwh.utils.utils import go_up_dirs, read_config, col_to_index
from datetime import datetime
# # -----------------------------
# # Import parse_headers
# # -----------------------------
# try:
#     parse_mod = importlib.import_module("parse_headers")
#     parse_header_fields = parse_mod.parse_header_fields
# except ImportError:
#     raise RuntimeError("parse_headers.py not found. Ensure it's in the same directory.")

# -----------------------------
# DB helpers
# -----------------------------

def reflect_tables(engine, schema="price_list"):
    """
    Helper: reflect_tables

    Uses SQLAlchemy reflection to load all tables defined in a given schema
    (e.g., from price_list_schema.sql) into a MetaData object.

    Workflow:
        - Creates a MetaData object tied to the target schema.
        - Calls .reflect() on the database engine, which queries system catalogs
          to discover all tables, columns, foreign keys, and indexes.
        - Returns the MetaData, allowing you to access tables by name through
          md.tables["schema.table_name"].

    Why:
        This ensures the Python code always matches the actual database schema,
        rather than re-defining tables in Python and risking mismatches.
        It lets you safely reference real tables like
        md.tables["price_list.price_list"] or md.tables["price_list.price_source"].
    """
    # Define metadata object for the given schema
    md = MetaData(schema=schema)
    # call reflect to load table definitions from the database
    md.reflect(bind=engine, schema=schema)
    return md


# TODO: Maybe generalize this to get_or_create_by_cols(conn, table, col_val_dict, id_col)
def get_or_create(conn, table: Table, name_value: str, name_col: str, id_col: str):
    """
    Helper: get_or_create

    Ensures a consistent foreign-key reference for lookup tables
    (price_categories, quote_type, price_source, unit_of_measure).

    Workflow:
        - Accepts a connection, a reflected SQLAlchemy Table, a string value,
        the column name to match on (e.g., 'name'), and the table’s ID column.
        - Attempts to INSERT the given value into the lookup table.
        - If the value already exists, uses PostgreSQL's ON CONFLICT to do a no-op update.
        - Always returns the primary key ID (via RETURNING), so the calling code
        can use it when inserting into price_list or related tables.

    Why:
        This prevents duplicate rows for things like vendors or units of measure,
        while also saving you from writing separate SELECT-then-INSERT logic.
        Effectively: "insert if new, else fetch the existing ID."
    """
    if not name_value:
        return None
    stmt = text(
        f"""
        INSERT INTO {table.fullname} ({name_col})
        VALUES (:name)
        ON CONFLICT ({name_col}) DO UPDATE SET {name_col}=EXCLUDED.{name_col}
        RETURNING {id_col}
        """
    )
    res = conn.execute(stmt, {"name": name_value})
    return int(res.scalar())

def get_or_create_category(conn, name_value: str, level_value: str) -> int | None:
    """
    Helper: get_or_create_category

    Ensures a consistent foreign-key reference in price_list.price_categories
    for a given (name, level) pair.

    Args:
        conn       : SQLAlchemy connection (inside a transaction block)
        name_value : Category name (e.g., "Lumber", "Dimensional Lumber")
        level_value: Category level (e.g., "1", "2", "3")

    Returns:
        The category_id (int) for the row, inserting if necessary.

    Behavior:
        • Attempts to INSERT (name, level).
        • If a row with the same name already exists, does nothing destructive
          but updates the level to the new value.
        • Always returns the primary key id via RETURNING.
    """
    if not name_value:
        return None

    stmt = text(
        """
        INSERT INTO price_list.price_categories (name, level)
        VALUES (:name, :level)
        ON CONFLICT (name, level) DO UPDATE SET name = EXCLUDED.name, level = EXCLUDED.level 
        RETURNING category_id
        """
    )
    res = conn.execute(stmt, {"name": name_value, "level": level_value})
    return int(res.scalar())

# -----------------------------
# Main ingest function
# -----------------------------

def ingest_price_list(
    xlsx_path: str,
    sheet_name: str = "Price_List",
    engine_url: str = "postgresql+psycopg2://user:pass@localhost:5432/midwest",
    schema: str = "price_list",
    header_row: int | None = None,
    index_cols: int = 1,
    BATCH_SIZE: int = 250,
):
    """
    Ingests the price list Excel file into the database schema defined by price_list_schema.sql.
    Uses parse_headers.parse_header_fields for parsing header metadata.
    """
    if isinstance(index_cols, str):
        index_cols = col_to_index(index_cols)

    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    # Define the natural key (uniqueness definition for price_list)
    NATURAL_KEY = ["category1_id", "category2_id", "category3_id", "quote_date", "price_source_id"]
    
    # Infer header row if not given
    if header_row is None:
        for i in range(min(30, len(df))):
            # walk down rows until we find one with at least 2 non-nan values after index_cols
            non_nan_after_idx = df.iloc[i, index_cols:].notna().sum()
            if non_nan_after_idx >= 2:
                header_row = i
                break
    if header_row is None:
        raise ValueError("Could not infer header row.")

    # Parse headers
    header_metas = {}
    for col in range(index_cols, df.shape[1]):
        hv = df.iat[header_row, col]
        if hv and isinstance(hv, str):
            header_metas[col] = parse_header_fields(hv)

    # Candidate rows
    body = df.iloc[header_row + 1 :].reset_index(drop=True)

    engine = create_engine(engine_url, future=True)
    md = reflect_tables(engine, schema=schema)

    price_list = md.tables.get(f"{schema}.price_list")
    categories = md.tables.get(f"{schema}.price_categories")
    quote_types = md.tables.get(f"{schema}.quote_type")
    price_sources = md.tables.get(f"{schema}.price_source")
    uoms = md.tables.get(f"{schema}.unit_of_measure")

    inserted = 0

    with engine.connect() as conn:
        trans = conn.begin()
        for r_idx in tqdm(range(len(body)), desc="Ingesting rows"):
        # for r_idx in range(len(body)):
            row = body.iloc[r_idx]
            # Assume first 3 columns are Lvl 1, Lvl 2, Lvl 3 categories
            # NOTE: Test this in operation and see if row['Lvl 1 Category'] etc. works
            lvl1 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
            lvl2 = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None
            lvl3 = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else None
            description = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else None

            thickness = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else None
            width = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else None
            length = str(row.iloc[7]).strip() if pd.notna(row.iloc[7]) else None

            for c_idx, hm in header_metas.items():
                val = row.iloc[c_idx]
                # Check for non-empty value
                if pd.isna(val) or str(val).strip() == "":
                    continue
                # Check for valid date
                event_date = hm.get("event_date")
                if event_date is None:
                    continue  # Skip rows without a valid date

                lvl1_id = get_or_create_category(conn, lvl1, "1")
                lvl2_id = get_or_create_category(conn, lvl2, "2")
                lvl3_id = get_or_create_category(conn, lvl3, "3")
                price_source_id = get_or_create(conn, price_sources, hm.get("event_source"), "name", "price_source_id") if "price_sources" in locals() else None
                uom_id = get_or_create(conn, uoms, hm.get("event_unit_measure"), "name", "unit_of_measure_id") if "uoms" in locals() else None
                if isinstance(event_date, str):
                    event_date = datetime.strptime(event_date, "%m/%d/%y").date()


                payload = {
                    "category1_id": lvl1_id,
                    "category2_id": lvl2_id,
                    "category3_id": lvl3_id,
                    "description": description,
                    "price_source_id": price_source_id,
                    "quote_date": event_date,
                    'dim_thickness': thickness,
                    'dim_width': width,
                    'dim_length': length,
                    "unit_of_measure_id": uom_id,
                    "price_value": float(val) if isinstance(val, (int, float)) else None,
                    "notes": None,
                }


                # Build an INSERT for price_list
                ins = pg_insert(price_list).values(**payload)

                # Exclude natural key columns from the update set
                update_cols = {k: ins.excluded[k] for k in payload.keys() if k not in NATURAL_KEY}

                # Add ON CONFLICT clause using the natural key
                stmt = ins.on_conflict_do_update(
                    index_elements=NATURAL_KEY,
                    set_=update_cols,
                )

                # Execute the upsert
                conn.execute(stmt)
                inserted += 1

                if inserted % BATCH_SIZE == 0:
                    print(f"Inserted/Updated {inserted} rows...")
                    trans.commit()
                    trans = conn.begin()
        trans.commit()

    return inserted

if __name__ == "__main__":
    import os

    # --- File input ---
    data_directory = r"C:\Users\austi\OneDrive\Documents\Contracts\Midwest Heritage\Data\working"
    price_list_name = "25.08.08_jobname_EstSheet_v25.08.04 (LamarTest).xlsx"
    path = os.path.join(data_directory, price_list_name)

    # --- Database connection from .ini ---
    db_ini = os.path.join(go_up_dirs(__file__, 2), "inputs", "connect2database.ini")
    params = read_config(db_ini, section="postgresql")

    # Build SQLAlchemy connection URL
    ENGINE = (
        f"postgresql+psycopg2://{params['user']}:{params['password']}"
        f"@{params['host']}:{params['port']}/{params['database']}"
    )

    # Run the ingest
    print(ingest_price_list(path, 
                            sheet_name="Price_List", 
                            engine_url=ENGINE, 
                            header_row=18, 
                            index_cols='ab'))
    # TODO: ADD the rest of the fields for the price list table and get this working. 