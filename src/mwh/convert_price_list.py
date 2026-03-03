import psycopg2
from psycopg2.extras import RealDictCursor
from parse_headers import parse_header_fields

DSN = "dbname=mwh user=postgres password=YOUR_PASSWORD host=localhost port=5432"

# ---------- generic helpers ----------
def get_or_create_by_name(cur, table, id_col, name_col, value):
    """
    Upsert a single 'name' into a dimension table and return its id.
    Requires a UNIQUE constraint on name_col.
    """
    sql = f"""
        INSERT INTO {table} ({name_col})
        VALUES (%s)
        ON CONFLICT ({name_col})
        DO UPDATE SET {name_col} = EXCLUDED.{name_col}
        RETURNING {id_col};
    """
    cur.execute(sql, (value,))
    return cur.fetchone()[0]

def get_or_create_item(cur, sku, description):
    """
    Upsert item based on (sku, description). See uq_item in schema.
    """
    sql = """
        INSERT INTO item (sku, description)
        VALUES (%s, %s)
        ON CONFLICT (COALESCE(sku, ''), COALESCE(description, ''))
        DO UPDATE SET sku = EXCLUDED.sku, description = EXCLUDED.description
        RETURNING item_id;
    """
    cur.execute(sql, (sku, description))
    return cur.fetchone()[0]

def get_or_create_header_event(cur, vendor_id, event_date, doc_type_id, metric_id, raw_header):
    """
    Upsert header_event using the composite natural key uq_header.
    """
    sql = """
        INSERT INTO header_event (vendor_id, event_date, doc_type_id, metric_id, raw_header)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (vendor_id, event_date, doc_type_id, metric_id, raw_header)
        DO UPDATE SET raw_header = EXCLUDED.raw_header
        RETURNING header_id;
    """
    cur.execute(sql, (vendor_id, event_date, doc_type_id, metric_id, raw_header))
    return cur.fetchone()[0]

def link_header_category(cur, header_id, category_id):
    """
    Bridge insert with dedupe via PK (header_id, category_id).
    """
    sql = """
        INSERT INTO header_event_category (header_id, category_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
    """
    cur.execute(sql, (header_id, category_id))

def upsert_fact_observation(cur, item_id, header_id, value):
    """
    Insert or replace the fact value for (item, header).
    If you prefer 'ignore if exists', change DO UPDATE to DO NOTHING.
    """
    sql = """
        INSERT INTO fact_observation (item_id, header_id, value)
        VALUES (%s, %s, %s)
        ON CONFLICT (item_id, header_id)
        DO UPDATE SET value = EXCLUDED.value
        RETURNING obs_id;
    """
    cur.execute(sql, (item_id, header_id, value))
    return cur.fetchone()[0]

# ---------- domain helpers ----------
def ensure_vendor(cur, name_or_none):
    return get_or_create_by_name(cur, "vendor", "vendor_id", "name", name_or_none) if name_or_none else None

def ensure_doc_type(cur, name_or_none):
    return get_or_create_by_name(cur, "doc_type", "doc_type_id", "name", name_or_none) if name_or_none else None

def ensure_source(cur, name_or_none):
    return get_or_create_by_name(cur, "source", "source_id", "name", name_or_none) if name_or_none else None

def ensure_metric(cur, name_or_none):
    return get_or_create_by_name(cur, "metric", "metric_id", "name", name_or_none) if name_or_none else None

def ensure_categories(cur, category_names):
    ids = []
    for nm in category_names or []:
        nm = (nm or "").strip()
        if not nm:
            continue
        ids.append(get_or_create_by_name(cur, "category", "category_id", "name", nm))
    return ids

# # Example header parser tailored to your patterns.
# # Returns dict: {vendor, event_date, doc_type, metric, categories, raw_header}
# import re
# from datetime import datetime

# PAT_VENDOR_DATE_CATS = re.compile(
#     r"""^\s*(?P<vendor>[A-Za-z@&\s]+)\s+(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s*
#         \(\s*(?P<cats>[^)]*)\)\s*$""", re.X)

# PAT_DATE_PRINT_METRIC = re.compile(
#     r"""^\s*(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s+Print\s+(?P<metric>\$|MBF\s*/\s*MSF)\s*$""", re.X)

# PAT_VENDOR_DATE_TAIL = re.compile(
#     r"""^\s*(?P<vendor>[A-Za-z@&\s]+)\s+(?P<date>\d{1,2}/\d{1,2}/\d{2,4})(?:\s+(?P<tail>.+))?$""", re.X)

# def parse_header(raw_header):
#     h = (raw_header or "").strip()

#     # Pattern 1: Vendor Date (cat1; cat2; ...)
#     m = PAT_VENDOR_DATE_CATS.match(h)
#     if m:
#         cats = [c.strip(" ;,") for c in m.group("cats").replace("&", ";").split(";") if c.strip()]
#         return {
#             "vendor": m.group("vendor").strip(),
#             "event_date": to_iso_date(m.group("date")),
#             "doc_type": None,
#             "metric": None,
#             "categories": cats,
#             "raw_header": h
#         }

#     # Pattern 2: Date Print $|MBF/MSF
#     m = PAT_DATE_PRINT_METRIC.match(h)
#     if m:
#         metric = "$" if "$" in m.group("metric") else "MBF/MSF"
#         return {
#             "vendor": None,
#             "event_date": to_iso_date(m.group("date")),
#             "doc_type": "Print",
#             "metric": metric,
#             "categories": [],
#             "raw_header": h
#         }

#     # Pattern 3: Vendor Date [tail tokens]
#     m = PAT_VENDOR_DATE_TAIL.match(h)
#     if m:
#         tail = (m.group("tail") or "").strip()
#         doc_type = None
#         metric = None
#         cats = []

#         if "Special Quote" in tail:
#             doc_type = "Special Quote"
#         elif "Quote" in tail and "Special" not in tail:
#             doc_type = "Quote"
#         if "$" in tail:
#             metric = "$"
#         elif "MBF" in tail or "MSF" in tail:
#             metric = "MBF/MSF"

#         return {
#             "vendor": m.group("vendor").strip(),
#             "event_date": to_iso_date(m.group("date")),
#             "doc_type": doc_type,
#             "metric": metric,
#             "categories": cats,
#             "raw_header": h
#         }

#     # Fallback: only date?
#     # (You can extend as you find more patterns.)
#     return {"vendor": None, "event_date": None, "doc_type": None, "metric": None, "categories": [], "raw_header": h}

# def to_iso_date(s):
#     # 1/6/22, 04/05/2024, etc.
#     m, d, y = s.split("/")
#     y = int(y)
#     # normalize 2-digit years (assume 20xx)
#     if y < 100:
#         y += 2000
#     return datetime(y, int(m), int(d)).date()

# ---------- end-to-end upsert for ONE header & ONE item value ----------
def upsert_one_cell(conn, raw_header, item_sku, item_description, value):
    """
    - parse header
    - ensure dimension rows
    - upsert header_event (+ categories bridge)
    - ensure item
    - upsert fact value
    """
    with conn:
        with conn.cursor() as cur:
            parsed = parse_header_fields(raw_header)
            if not parsed.get("event_date"):
                raise ValueError(f"Could not parse a date from header: {raw_header}")

            # vendor_id = ensure_vendor(cur, parsed["event_source"])
            source_id = ensure_source(cur, parsed['event_source'])
            doc_type_id = ensure_doc_type(cur, parsed["doc_type"])
            metric_id = ensure_metric(cur, parsed["metric"])

            header_id = get_or_create_header_event(
                cur,
                source_id,
                parsed["event_date"],
                doc_type_id,
                metric_id,
                parsed["raw_header"]
            )

            for cat_id in ensure_categories(cur, parsed["categories"]):
                link_header_category(cur, header_id, cat_id)

            item_id = get_or_create_item(cur, item_sku, item_description)

            obs_id = upsert_fact_observation(cur, item_id, header_id, value)
            return {"item_id": item_id, "header_id": header_id, "obs_id": obs_id}

# ---------- demo ----------
if __name__ == "__main__":
    conn = psycopg2.connect(DSN, cursor_factory=RealDictCursor)

    # Example 1: "Grabers 05/01/25 (SPF; Trtd)"
    r1 = upsert_one_cell(
        conn,
        raw_header="Grabers 05/01/25 (SPF; Trtd)",
        item_sku=None,
        item_description="2x4 board",
        value=345
    )
    print("Inserted/updated:", r1)

    # Example 2: "04/11/25 Print $"
    r2 = upsert_one_cell(
        conn,
        raw_header="04/11/25 Print $",
        item_sku=None,
        item_description="2x4 board",
        value=120
    )
    print("Inserted/updated:", r2)

    conn.close()
