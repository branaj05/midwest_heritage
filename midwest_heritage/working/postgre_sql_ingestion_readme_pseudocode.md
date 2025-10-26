# PostgreSQL Ingestion – README & Pseudocode

This document explains the data model, setup, and end‑to‑end ingestion flow for converting a wide spreadsheet with composite column headers (e.g., `Grabers 05/01/25 (SPF; Trtd)` or `04/11/25 Print $`) into a normalized PostgreSQL schema. It also includes detailed pseudocode that mirrors the Python file in the canvas.

---

## 1) Problem Summary
Your spreadsheet has **rows** (items/products) and **columns** whose **headers** bundle multiple variables (vendor, date, doc type, metric, categories). A single cell value needs to be stored along with all the variables encoded by its column header.

SQL prefers **tidy/long** data: each variable in its own column, each observation as a row.

---

## 2) Target Schema (Normalized)

### Lookup/Dimensions
- **vendor(vendor_id PK, name UNIQUE)**
- **doc_type(doc_type_id PK, name UNIQUE)** — e.g., `Print`, `Special Quote`, `Quote`
- **metric(metric_id PK, name UNIQUE)** — e.g., `$`, `MBF/MSF`
- **category(category_id PK, name UNIQUE)** — e.g., `OSB`, `SPF`, `Trtd`

### Event (Parsed Header)
- **header_event(header_id PK, vendor_id FK, event_date DATE, doc_type_id FK, metric_id FK, raw_header TEXT)**
  - **Unique constraint** across the natural key you care about — suggested:
    - `(vendor_id, event_date, doc_type_id, metric_id, raw_header)`

### Event ↔ Category Bridge (Many‑to‑Many)
- **header_event_category(header_id FK, category_id FK, PK (header_id, category_id))**

### Items (Spreadsheet Rows)
- **item(item_id PK, sku, description, UNIQUE(COALESCE(sku,''), COALESCE(description,'')))**

### Facts (Cell Values)
- **fact_observation(obs_id PK, item_id FK, header_id FK, value NUMERIC, UNIQUE(item_id, header_id))**

> The **UNIQUE** constraints make the ingestion idempotent and safe to run repeatedly.

---

## 3) Setup Instructions

1. **Create database & user** (example):
   ```sql
   CREATE DATABASE mwh;
   CREATE USER mwh_user WITH PASSWORD 'strong_password';
   GRANT ALL PRIVILEGES ON DATABASE mwh TO mwh_user;
   ```

2. **Connect and create tables** using the DDL above (or your existing migration tool). Make sure your connection user has rights to create schema objects.

3. **Install Python deps**:
   ```bash
   pip install psycopg2-binary
   ```

4. **Configure DSN** in your script (host, dbname, user, password, port).

---

## 4) Data Flow Overview

**Per spreadsheet file**
1. Load the sheet into memory (e.g., pandas) — keep leftmost ID columns (item identifiers) and treat the rest as value columns.
2. **Unpivot** (melt) so each cell becomes a row `(item identifiers, raw_header, value)`.
3. For each row:
   - Parse `raw_header` → `(vendor, date, doc_type, metric, categories[])`.
   - Get-or-create dimension IDs (vendor, doc_type, metric, category[]).
   - Get-or-create the `header_event` (one per unique parsed header).
   - Link categories via `header_event_category`.
   - Get-or-create the `item` (from your row identifiers).
   - Upsert the `fact_observation` using `(item_id, header_id) → value`.

Everything runs inside transactions; `ON CONFLICT` ensures dedupe.

---

## 5) Pseudocode (End‑to‑End)

> This mirrors the Python helpers in the current canvas.

### 5.1 Helpers: Get‑or‑Create
```
function get_or_create_by_name(cur, table, id_col, name_col, value):
    SQL = "INSERT INTO {table} ({name_col}) VALUES (?)\n" +
          "ON CONFLICT ({name_col}) DO UPDATE SET {name_col} = EXCLUDED.{name_col}\n" +
          "RETURNING {id_col};"
    execute SQL with [value]
    return returned {id_col}

function ensure_vendor(cur, vendor_name_or_null):
    if vendor_name_or_null is null: return null
    return get_or_create_by_name(cur, 'vendor', 'vendor_id', 'name', vendor_name_or_null)

function ensure_doc_type(cur, doc_type_name_or_null):
    if doc_type_name_or_null is null: return null
    return get_or_create_by_name(cur, 'doc_type', 'doc_type_id', 'name', doc_type_name_or_null)

function ensure_metric(cur, metric_name_or_null):
    if metric_name_or_null is null: return null
    return get_or_create_by_name(cur, 'metric', 'metric_id', 'name', metric_name_or_null)

function ensure_categories(cur, category_name_list):
    ids = []
    for name in category_name_list (or empty list):
        cleaned = trim(name)
        if cleaned != '':
            ids.append(get_or_create_by_name(cur, 'category', 'category_id', 'name', cleaned))
    return ids

function get_or_create_item(cur, sku, description):
    SQL = "INSERT INTO item (sku, description) VALUES (?, ?)\n" +
          "ON CONFLICT (COALESCE(sku,''), COALESCE(description,''))\n" +
          "DO UPDATE SET sku = EXCLUDED.sku, description = EXCLUDED.description\n" +
          "RETURNING item_id;"
    execute SQL with [sku, description]
    return returned item_id
```

### 5.2 Header Event Upsert + Bridge
```
function get_or_create_header_event(cur, vendor_id, event_date, doc_type_id, metric_id, raw_header):
    SQL = "INSERT INTO header_event (vendor_id, event_date, doc_type_id, metric_id, raw_header)\n" +
          "VALUES (?, ?, ?, ?, ?)\n" +
          "ON CONFLICT (vendor_id, event_date, doc_type_id, metric_id, raw_header)\n" +
          "DO UPDATE SET raw_header = EXCLUDED.raw_header\n" +
          "RETURNING header_id;"
    execute SQL with [vendor_id, event_date, doc_type_id, metric_id, raw_header]
    return returned header_id

function link_header_category(cur, header_id, category_id):
    SQL = "INSERT INTO header_event_category (header_id, category_id) VALUES (?, ?)\n" +
          "ON CONFLICT DO NOTHING;"
    execute SQL with [header_id, category_id]
```

### 5.3 Fact Upsert
```
function upsert_fact_observation(cur, item_id, header_id, value):
    SQL = "INSERT INTO fact_observation (item_id, header_id, value) VALUES (?, ?, ?)\n" +
          "ON CONFLICT (item_id, header_id) DO UPDATE SET value = EXCLUDED.value\n" +
          "RETURNING obs_id;"
    execute SQL with [item_id, header_id, value]
    return returned obs_id
```

### 5.4 Header Parsing (Regex‑style)
```
function parse_header(raw_header):
    h = trim(raw_header)

    # Pattern A: "Vendor Date (cat1; cat2; ...)"
    if matches(h, "^(VENDOR) (DATE) \((CATS)\)$"):
        vendor = group('VENDOR')
        date   = parse_date(group('DATE'))
        cats   = split_semicolons(group('CATS'))  # clean and trim
        return { vendor, event_date: date, doc_type: null, metric: null, categories: cats, raw_header: h }

    # Pattern B: "Date Print $|MBF/MSF"
    if matches(h, "^(DATE) Print (\$|MBF\s*/\s*MSF)$"):
        date   = parse_date(group('DATE'))
        metric = (group contains '$') ? '$' : 'MBF/MSF'
        return { vendor: null, event_date: date, doc_type: 'Print', metric, categories: [], raw_header: h }

    # Pattern C: "Vendor Date [tail]"  (tail may contain 'Quote', 'Special Quote', '$', 'MBF/MSF')
    if matches(h, "^(VENDOR) (DATE) (TAIL)?$"):
        vendor = group('VENDOR')
        date   = parse_date(group('DATE'))
        tail   = group('TAIL') or ''
        doc_type = (contains(tail, 'Special Quote') ? 'Special Quote' : contains(tail, 'Quote') ? 'Quote' : null)
        metric   = contains(tail, '$') ? '$' : (contains(tail, 'MBF') or contains(tail, 'MSF')) ? 'MBF/MSF' : null
        return { vendor, event_date: date, doc_type, metric, categories: [], raw_header: h }

    # Fallback
    return { vendor: null, event_date: null, doc_type: null, metric: null, categories: [], raw_header: h }
```

### 5.5 End‑to‑End for One Cell (One Header × One Item)
```
function upsert_one_cell(conn, raw_header, item_sku, item_description, value):
    begin transaction
        parsed = parse_header(raw_header)
        if parsed.event_date is null:
            raise "Unparseable header (missing date)"

        vendor_id   = ensure_vendor(cur, parsed.vendor)
        doc_type_id = ensure_doc_type(cur, parsed.doc_type)
        metric_id   = ensure_metric(cur, parsed.metric)

        header_id = get_or_create_header_event(cur,
                         vendor_id,
                         parsed.event_date,
                         doc_type_id,
                         metric_id,
                         parsed.raw_header)

        category_ids = ensure_categories(cur, parsed.categories)
        for each cid in category_ids:
            link_header_category(cur, header_id, cid)

        item_id = get_or_create_item(cur, item_sku, item_description)

        obs_id = upsert_fact_observation(cur, item_id, header_id, value)
    commit
    return { item_id, header_id, obs_id }
```

### 5.6 Batch Ingestion (Whole Spreadsheet)
```
function ingest_sheet(path_to_excel):
    df = load_excel(path_to_excel)

    # Choose which left-side columns uniquely identify an item (e.g., sku, description)
    id_cols = ['sku', 'description']

    # Melt wide → long: columns become rows under 'raw_header'
    long_df = melt(df, id_vars=id_cols, var_name='raw_header', value_name='value')

    # Drop empty cells
    long_df = drop_na(long_df, columns=['value'])

    open connection as conn
    for row in long_df:
        upsert_one_cell(conn,
                        raw_header=row['raw_header'],
                        item_sku=row['sku'],
                        item_description=row['description'],
                        value=row['value'])
    close connection
```

---

## 6) Example Queries

- **All Grabers values in March–May 2025 for SPF:**
```sql
SELECT i.description, he.event_date, f.value
FROM fact_observation f
JOIN item i ON i.item_id = f.item_id
JOIN header_event he ON he.header_id = f.header_id
JOIN vendor v ON v.vendor_id = he.vendor_id
JOIN header_event_category hec ON hec.header_id = he.header_id
JOIN category c ON c.category_id = hec.category_id
WHERE v.name = 'Grabers'
  AND he.event_date BETWEEN '2025-03-01' AND '2025-05-31'
  AND c.name = 'SPF'
ORDER BY he.event_date;
```

- **Compare `$` vs `MBF/MSF` in a date range:**
```sql
SELECT m.name AS metric, he.event_date, AVG(f.value) AS avg_val
FROM fact_observation f
JOIN header_event he ON he.header_id = f.header_id
LEFT JOIN metric m ON m.metric_id = he.metric_id
WHERE he.event_date BETWEEN '2025-01-01' AND '2025-12-31'
GROUP BY m.name, he.event_date
ORDER BY he.event_date, m.name;
```

---

## 7) Error Handling & Idempotency
- **Parsing failures**: if a header lacks a date or doesn’t match patterns, log and skip or raise.
- **Transactions**: wrap each cell or each batch chunk in a transaction.
- **Conflicts**: `ON CONFLICT` ensures you don’t duplicate vendors, headers, links, or facts.
- **Logging**: capture `(raw_header, row_id)` when errors occur to debug easily.

---

## 8) Extending the Parser
- Normalize vendor names (e.g., `Graber` vs `Grabers`). Consider a small mapping table.
- Handle commas vs semicolons within category lists and mixed capitalization.
- Add patterns for special cases (e.g., `Statesboro`, `Seasons@Plainfield`).

---

## 9) Alternatives & Notes
- **SQLAlchemy**: you can port the same logic to Core/ORM for model classes and session management.
- **Staging table**: optionally stage raw melted data first; then run a SQL procedure to populate dimensions/facts.
- **Performance**: batch inserts (use `execute_batch`/`copy_from`) once your logic stabilizes.

---

## 10) Glossary
- **Get‑or‑Create**: Try to insert; on conflict (unique), fetch the existing row’s ID.
- **Bridge table**: Resolves many‑to‑many relationships (e.g., header ↔ multiple categories).
- **Idempotent**: Running the same ingestion twice yields the same final database state.

---

## 11) Next Steps
- Decide the **item identifiers** (which columns uniquely identify a row).
- Finalize the **unique key** for `header_event` (include/exclude `raw_header`).
- Add unit tests for `parse_header` across your real header variants.
- (Optional) Build a small report/query layer (Metabase) for non‑technical stakeholders.

