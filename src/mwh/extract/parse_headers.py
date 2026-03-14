#%%
import re
import os
import pandas as pd
from datetime import datetime

# ---------- 1) Patterns (independent) ----------
# Vendor: begin-of-string token; allow spaces & common punctuation
#     Pattern looks for a vendor name at the start of the string. 
#     Includes lookahead to ensure it's followed by a date, double space+, or hyphen. 
RX_VENDOR = re.compile(r"^\s*(?P<vendor>[A-Za-z][^\d/\-]{2,}?)\s*(?=\-|\s+\d{1,2}/\d{1,2}/\d{2,4}|\s{2,})",re.IGNORECASE)

# Date: MM/DD/YY or MM/DD/YYYY anywhere
RX_DATE = re.compile(r"(?P<date>\b\d{1,2}/\d{1,2}/\d{2,4}\b)")

# Categories: anything inside (...) anywhere (greedy outer; we’ll split inside)
RX_CATS = re.compile(r"\((?P<cats>[^)]*)\)")

# Doc type: “Special Quote” takes precedence over “Quote”; also “Print”
RX_DOCTYPE_SPECIAL = re.compile(r"\bSpecial\s+Quote?", re.IGNORECASE)
RX_DOCTYPE_QUOTE   = re.compile(r"\bQuote\b", re.IGNORECASE)
RX_DOCTYPE_PRINT   = re.compile(r"\bPrint\b", re.IGNORECASE)

# Metric: $ or MBF/MSF (allow spaces around slash)
RX_METRIC = re.compile(r"(?P<metric>\$|MBF\s*/\s*MSF)", re.IGNORECASE)

# ---------- 2) Helpers ----------
def parse_date_any(s):
    m = RX_DATE.search(s)
    if not m:
        return None
    mm, dd, yy = m.group("date").split("/")
    yy = int(yy)
    if yy < 100:  # 2-digit year -> 20xx
        yy += 2000
    return datetime(yy, int(mm), int(dd)).strftime("%m/%d/%y")

def parse_vendor(s):
    m = RX_VENDOR.search(s)
    if not m:
        return None
    raw = m.group('vendor').strip()
    return m.group("vendor").strip() if m else None

def parse_categories(s):
    """
    Grab the FIRST (...) block as categories; if multiple exist and you care,
    change to RX_CATS.findall(s) and merge.
    Split on ; or , or &; trim punctuation.
    """
    m = RX_CATS.search(s)
    if not m:
        return []
    raw = m.group("cats")
    # normalize delimiters ; , &
    raw = raw.replace("&", ";")
    for ch in [","]:
        raw = raw.replace(ch, ";")
    toks = [t.strip(" ;,") for t in raw.split(";")]
    return [t for t in toks if t]

def parse_doc_type(s):
    if RX_DOCTYPE_SPECIAL.search(s): return "Special Quote"
    if RX_DOCTYPE_PRINT.search(s):   return "Print"
    if RX_DOCTYPE_QUOTE.search(s):   return "Quote"
    return None

def parse_metric(s, default="$"):
    m = RX_METRIC.search(s)
    if not m: 
        return default
    return "$" if "$" in m.group(0) else "MBF/MSF"

def parse_header_combo(parser1, parser2, s):
    """
    Try parser1 first; if it returns None, try parser2.
    Useful for cases like combining 'vendor' and 'doc_type' where they're usually 
    mutually exclusive but need to be parsed using different patterns. Since they're mutually
    exclusive, we can try one and then the other and combine them into one category.
    """
    r = parser1(s)
    if r is not None:
        return r
    return parser2(s)
# ---------- 3) Unified field extractor ----------
def parse_header_fields(header: str):
    h = (header or "").strip()
    vendor   = parse_vendor(h)
    doc_type = parse_doc_type(h)
    if vendor and doc_type:
        event_source = f"{vendor} {doc_type}"
    else:
        event_source = vendor or doc_type
    return {
        "event_source":       event_source,
        "event_date":         parse_date_any(h),
        "event_unit_measure": parse_metric(h),
        "raw_header":         h,
    }

# ---------- 4) Examples ----------
if __name__ == "__main__":
    # Quick Test of an Example Set
    if False: 
        examples = [
            "Grabers 04/01/25 (SPF, SYP, OSB, Zip, Plywood, Trtd, GRK)",
            "04/11/25 Print $",
            "Matheus 12/29/22 MBF / MSF",
            "Seasons@Plainfield 02/07/22 Special Quote",
            "Shelter Products 10/11/23 (Statesboro)"
        ]

        storage = pd.DataFrame([parse_header_fields(e) for e in examples])
        print(storage)
    if True: 
        # Read in Price List from Excel and Parse Headers
        data_directory = r"C:\Users\austi\OneDrive\Documents\Contracts\Midwest Heritage\Data\working"
        price_list_name = "25.08.08_jobname_EstSheet_v25.08.04 (LamarTest).xlsx"
        price_list_path = os.path.join(data_directory, price_list_name)
        print(f"Reading {price_list_path}...")
        price_list = pd.read_excel(price_list_path, 
                                sheet_name='Price_List', 
                                skiprows=18, #TODO: Find this automatically
                                engine='openpyxl')
        print("Parsing headers...")
        # TODO: Automatically find the start column of headers
        # (currently hardcoded to column 28 / 'AB')
        cols2parse = [c for c in price_list.columns[28: ] if isinstance(c, str) and c.strip()]
        storage = pd.DataFrame([parse_header_fields(e) for e in cols2parse])
        print('Saving...')
        storage.to_csv(os.path.join(data_directory, "parsed_price_list_headers.csv"), index=False)
        print('Done.')


