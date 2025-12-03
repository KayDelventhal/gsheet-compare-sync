# -*- coding: utf-8 -*-
"""
Core logic for CompareAndSyncSheets.
Contains data clients (Sheets, TSV) and comparison algorithms.
"""

from __future__ import annotations
import os
import re
import time
import random
import datetime as dt
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional

# --- Google Sheets ---
import gspread
from google.oauth2.service_account import Credentials

# --- Constants ---
WRITE_DELAY = 0.2
DIM_BLEND_FACTOR = 0.30
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}

# --- Helpers ---
_money_re = re.compile(r"[€$£]\s*")
_only_digits_comma_dot = re.compile(r"^[\d\.,]+$")

def _to_number_if_possible(val: Any) -> Any:
    if val is None: return None
    if isinstance(val, (int, float)): return float(val)
    s = str(val).strip()
    if not s: return ""
    s = _money_re.sub("", s).strip()
    if _only_digits_comma_dot.match(s):
        last_comma, last_dot = s.rfind(","), s.rfind(".")
        if last_comma > -1 and last_dot > -1:
            if last_comma > last_dot: s = s.replace(".", "").replace(",", ".")
            else: s = s.replace(",", "")
        elif last_comma > -1:
            s = s.replace(",", ".")
        try: return float(s)
        except Exception: pass
    try: return float(s)
    except Exception: return val

def _to_date_iso_if_possible(val: Any) -> Any:
    if val is None: return None
    if isinstance(val, dt.date): return val.isoformat()
    s = str(val).strip()
    if not s: return ""
    for f in ["%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]:
        try: return dt.datetime.strptime(s, f).date().isoformat()
        except Exception: continue
    return val

def normalize_cell(val: Any) -> Any:
    if val is None: return ""
    v = _to_date_iso_if_possible(val)
    if v is not val: return v
    v2 = _to_number_if_possible(val)
    if isinstance(v2, float): return round(v2, 10)
    return str(val).strip()

def a1_cell(row0: int, col0: int) -> str:
    """Converts 0-based row/col to A1 notation."""
    s, n = "", col0 + 1
    while n > 0: n, r = divmod(n - 1, 26); s = chr(65 + r) + s
    return f"{s}{row0 + 1}"

def rgb_to_hsv(r, g, b):
    max_c = max(r, g, b)
    min_c = min(r, g, b)
    diff = max_c - min_c
    h = s = v = 0
    v = max_c
    if max_c > 0:
        s = diff / max_c
    if diff > 0:
        if max_c == r: h = (g - b) / diff
        elif max_c == g: h = 2 + (b - r) / diff
        else: h = 4 + (r - g) / diff
        h *= 60
        if h < 0: h += 360
    return h, s, v

def hsv_to_rgb(h, s, v):
    if s == 0: return v, v, v
    h /= 60
    i = int(h)
    f = h - i
    p = v * (1 - s)
    q = v * (1 - s * f)
    t = v * (1 - s * (1 - f))
    if i == 0: return v, t, p
    if i == 1: return q, v, p
    if i == 2: return p, v, t
    if i == 3: return p, q, v
    if i == 4: return t, p, v
    return v, p, q

def is_white(color: Optional[Dict[str, float]]) -> bool:
    if not color: return True
    r = color.get('red', 0.0)
    g = color.get('green', 0.0)
    b = color.get('blue', 0.0)
    return r > 0.9 and g > 0.9 and b > 0.9

# --- Clients ---

class SheetsClient:
    def __init__(self, credentials_path: str):
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
        creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
        self.gc = gspread.authorize(creds)

    def _retry_api(self, func, *args, **kwargs):
        for attempt in range(5):
            try:
                time.sleep(WRITE_DELAY)
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                err_str = str(e)
                if any(code in err_str for code in ["429", "500", "503"]):
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    print(f"API Error ({err_str}), retrying in {wait:.2f}s...")
                    time.sleep(wait)
                    continue
                raise
        raise

    def _open_sheet(self, spreadsheet_id_or_name: str) -> gspread.Spreadsheet:
        try: return self.gc.open_by_key(spreadsheet_id_or_name)
        except gspread.SpreadsheetNotFound: return self.gc.open(spreadsheet_id_or_name)

    def list_worksheets(self, spreadsheet_id: str) -> List[str]:
        return [ws.title for ws in self._open_sheet(spreadsheet_id).worksheets()]

    def fetch_values(self, spreadsheet_id: str, worksheet_title: str) -> Tuple[List[str], List[List[Any]]]:
        ws = self._open_sheet(spreadsheet_id).worksheet(worksheet_title)
        values = self._retry_api(ws.get_all_values)
        return ([h.strip() for h in values[0]], values[1:]) if values else ([], [])

    def fetch_formats(self, spreadsheet_id: str, worksheet_title: str) -> List[Dict]:
        sh = self._open_sheet(spreadsheet_id)
        ws = sh.worksheet(worksheet_title)
        end_col = gspread.utils.rowcol_to_a1(1, ws.col_count).rstrip('1')
        fetch_range = f"'{worksheet_title}'!A2:{end_col}"
        params = {
            'ranges': fetch_range,
            'includeGridData': True,  # Required to actually return rowData/effectiveFormat
            'fields': 'sheets.data(startRow,startColumn,rowData.values.effectiveFormat.backgroundColor)',
        }
        try:
            meta = self._retry_api(sh.fetch_sheet_metadata, params=params)
            if 'sheets' in meta and meta['sheets'] and 'data' in meta['sheets'][0] and meta['sheets'][0]['data']:
                return meta['sheets'][0]['data'][0].get('rowData', [])
            return []
        except Exception as e:
            print(f"Error fetching formats: {e}")
            return []

    def batch_update(self, spreadsheet_id: str, body: Dict):
        if not body.get('requests'): return
        sh = self._open_sheet(spreadsheet_id)
        self._retry_api(sh.batch_update, body)

    def batch_update_values(self, spreadsheet_id: str, worksheet_title: str, updates: List[Tuple[int, int, Any]]):
        if not updates: return
        sh = self._open_sheet(spreadsheet_id)
        ws = sh.worksheet(worksheet_title)
        data = [{"range": f"'{ws.title}'!{a1_cell(r, c)}", "values": [[str(v) if v is not None else ""]]} for r, c, v in updates]
        self._retry_api(ws.spreadsheet.values_batch_update, body={"valueInputOption": "USER_ENTERED", "data": data})

class TSVClient:
    def fetch_values(self, file_path: str) -> Tuple[List[str], List[List[Any]]]:
        if not os.path.isfile(file_path): raise FileNotFoundError(f"TSV file not found: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f: lines = [ln.rstrip("\n") for ln in f]
        if not lines: return [], []
        rows = [ln.split("\t") for ln in lines]
        return [h.strip() for h in rows[0]], rows[1:]

# --- Comparison Logic ---

class CompareResult:
    def __init__(self):
        self.missing_rows_in_target: List[str] = []
        self.missing_rows_in_source: List[str] = []
        self.missing_columns_in_target: List[str] = []
        self.missing_columns_in_source: List[str] = []
        self.compared_headers: List[str] = []
        # Dict[key, List[Tuple[header, s_val, t_val, s_row_idx, t_row_idx, s_col_idx, t_col_idx]]]
        self.differences: Dict[str, List[Tuple[str, Any, Any, int, int, int, int]]] = {}
        self.row_mapping: Dict[str, Tuple[int, int]] = {} # Key -> (s_row, t_row)

    def to_report(self) -> str:
        lines = []
        if self.compared_headers: lines.append("Compared columns: " + ", ".join(sorted(self.compared_headers)))
        if self.missing_columns_in_target: lines.append("Columns missing in TARGET: " + ", ".join(self.missing_columns_in_target))
        if self.missing_columns_in_source: lines.append("Columns missing in SOURCE: " + ", ".join(self.missing_columns_in_source))
        if self.missing_rows_in_target: lines.append("\nRows missing in TARGET (SOURCE only):\n" + "\n".join(f"\t- {k}" for k in self.missing_rows_in_target))
        if self.missing_rows_in_source: lines.append("\nRows missing in SOURCE (TARGET only):\n" + "\n".join(f"\t- {k}" for k in self.missing_rows_in_source))
        if self.differences:
            lines.append("\nDifferences by key/header:")
            for key, diffs in self.differences.items():
                s_row, t_row = diffs[0][3], diffs[0][4]
                lines.append(f"  [{key}] | Source Row: {s_row}, Target Row: {t_row}")
                for header, sv, tv, _, _, _, _ in diffs: lines.append(f"\t- {header}: '{sv}' vs '{tv}'")
        return "\n".join(lines) if lines else "No differences found."

def compare_two_sheets(s_h, s_r, t_h, t_r, key_h, included_h):
    res = CompareResult()
    src_hmap = {h: i for i, h in enumerate(s_h)}
    tgt_hmap = {h: i for i, h in enumerate(t_h)}
    if key_h not in src_hmap: raise ValueError(f"Key header '{key_h}' not found in source.")
    if key_h not in tgt_hmap: raise ValueError(f"Key header '{key_h}' not found in target.")
    
    res.missing_columns_in_target = sorted([h for h in src_hmap if h not in tgt_hmap])
    res.missing_columns_in_source = sorted([h for h in tgt_hmap if h not in src_hmap])

    included_set = {h.strip() for h in included_h}
    common_headers = [h for h in src_hmap if h in tgt_hmap and h != key_h and h in included_set]
    res.compared_headers = sorted(list(common_headers))

    def index_rows(rows, hmap):
        key_col = hmap[key_h]
        key_to_idx, key_to_vals = {}, {}
        for i, row in enumerate(rows):
            if key_col < len(row) and (key_val := str(row[key_col]).strip()):
                # Store 1-based index (Header is 1, first data row is 2)
                key_to_idx[key_val], key_to_vals[key_val] = i + 2, row
        return key_to_idx, key_to_vals

    src_key2idx, src_key2vals = index_rows(s_r, src_hmap)
    tgt_key2idx, tgt_key2vals = index_rows(t_r, tgt_hmap)
    
    src_keys, tgt_keys = set(src_key2idx.keys()), set(tgt_key2idx.keys())
    res.missing_rows_in_target = sorted(list(src_keys - tgt_keys))
    res.missing_rows_in_source = sorted(list(tgt_keys - src_keys))

    for k in sorted(src_keys & tgt_keys):
        srow, trow = src_key2vals[k], tgt_key2vals[k]
        srow_idx, trow_idx = src_key2idx[k], tgt_key2idx[k]
        res.row_mapping[k] = (srow_idx, trow_idx)
        diffs = []
        for h in common_headers:
            sc, tc = src_hmap[h], tgt_hmap[h]
            sv = srow[sc] if sc < len(srow) else ""
            tv = trow[tc] if tc < len(trow) else ""
            if normalize_cell(sv) != normalize_cell(tv):
                diffs.append((h, sv, tv, srow_idx, trow_idx, sc, tc))
        if diffs: res.differences[k] = diffs
    return res

def check_color_status(result: CompareResult, current_formats: List[Dict], t_h: List[str], included_h: List[str]) -> List[str]:
    """
    Checks two conditions:
    1. Missing Color: A cell has a data difference but is WHITE.
    2. False Positive: A cell is COLORED but has NO data difference (data matches).
    
    Only checks within the 'included_h' columns to avoid flagging unrelated manual formatting.
    """
    report = []
    
    # 1. Identify all colored cells in the sheet (that fall under 'included_h')
    # Set of (row_0based, col_0based)
    actually_colored_cells = set() 
    
    tgt_hmap = {h: i for i, h in enumerate(t_h)}
    included_col_indices = {tgt_hmap[h] for h in included_h if h in tgt_hmap}

    # Parse current_formats (starts from row index 1, because header is row 0)
    for r_offset, row_data in enumerate(current_formats):
        real_row_idx = r_offset + 1 
        if 'values' not in row_data: continue
        
        for c_idx, cell_data in enumerate(row_data['values']):
            if c_idx not in included_col_indices: continue # Skip columns we aren't comparing
            
            if not cell_data or 'effectiveFormat' not in cell_data: continue
            color = cell_data['effectiveFormat'].get('backgroundColor')
            if not is_white(color):
                actually_colored_cells.add((real_row_idx, c_idx))

    # 2. Identify all cells that SHOULD be colored (Data differences)
    should_be_colored = set()
    # Map needed for reporting False Positives later: (row, col) -> (Key, Header)
    cell_info_map = {} 

    # Populate from result.differences
    for key_val, diffs in result.differences.items():
        for h, s_val, t_val, s_row, t_row, s_col, t_col in diffs:
            # t_row is 1-based
            target_row_0based = t_row - 1
            target_col_0based = t_col
            
            coord = (target_row_0based, target_col_0based)
            should_be_colored.add(coord)
            
            # Check for MISSING COLOR immediately
            if coord not in actually_colored_cells:
                 cell_ref = a1_cell(target_row_0based, target_col_0based)
                 report.append(f"[MISSING COLOR] Cell {cell_ref} (Row {t_row}, {h}): Has difference but is WHITE.")

    # 3. Check for FALSE POSITIVES (Colored but shouldn't be)
    # We iterate through all colored cells found in the "Columns to Compare"
    # REMOVED per user request: We only want to know if diffs are NOT colored.
    # We do not care if non-diffs ARE colored (stale colors).
    

    if not report:
        return ["Colors are perfectly synced with data differences."]
        
    return sorted(report)

def get_bg_color(row_data: Dict, col_idx: int) -> Optional[Dict]:
    if 'values' not in row_data: return None
    vals = row_data['values']
    if col_idx >= len(vals): return None
    cell = vals[col_idx]
    if not cell or 'effectiveFormat' not in cell: return None
    return cell['effectiveFormat'].get('backgroundColor')

def get_color_tuple(c: Optional[Dict]) -> Tuple[float, float, float]:
    if not c: return (1.0, 1.0, 1.0) # Default to white if missing
    return (c.get('red', 0.0), c.get('green', 0.0), c.get('blue', 0.0))

def colors_match(c1: Optional[Dict], c2: Optional[Dict], tolerance: float = 0.03) -> bool:
    # 1. Handle White/None equivalence
    w1 = is_white(c1)
    w2 = is_white(c2)
    if w1 and w2: return True
    if w1 != w2: return False
    
    # 2. Compare RGB components
    r1, g1, b1 = get_color_tuple(c1)
    r2, g2, b2 = get_color_tuple(c2)
    
    return (abs(r1 - r2) <= tolerance and 
            abs(g1 - g2) <= tolerance and 
            abs(b1 - b2) <= tolerance)

def compare_sheet_colors(result: CompareResult, s_formats: List[Dict], t_formats: List[Dict], s_h: List[str], t_h: List[str], included_h: List[str]) -> List[str]:
    report = []
    src_hmap = {h: i for i, h in enumerate(s_h)}
    tgt_hmap = {h: i for i, h in enumerate(t_h)}
    
    for key, (s_row, t_row) in result.row_mapping.items():
        # s_row, t_row are 1-based indices (1=Header, 2=First Data Row)
        # formats list starts from Data Row 1 (which is index 0 in list)
        s_idx = s_row - 2
        t_idx = t_row - 2
        
        if s_idx < 0 or s_idx >= len(s_formats): continue
        if t_idx < 0 or t_idx >= len(t_formats): continue
        
        s_row_data = s_formats[s_idx]
        t_row_data = t_formats[t_idx]
        
        for h in included_h:
            if h not in src_hmap or h not in tgt_hmap: continue
            s_col = src_hmap[h]
            t_col = tgt_hmap[h]
            
            s_color = get_bg_color(s_row_data, s_col)
            t_color = get_bg_color(t_row_data, t_col)
            
            if not colors_match(s_color, t_color):
                cell_ref = a1_cell(t_row - 1, t_col)
                
                s_white = is_white(s_color)
                t_white = is_white(t_color)
                
                if s_white and not t_white:
                    desc = "Source is White, Target is Colored"
                elif not s_white and t_white:
                    desc = "Source is Colored, Target is White"
                else:
                    # Both colored but different
                    sr, sg, sb = get_color_tuple(s_color)
                    tr, tg, tb = get_color_tuple(t_color)
                    desc = f"RGB Mismatch: Src({sr:.2f},{sg:.2f},{sb:.2f}) vs Tgt({tr:.2f},{tg:.2f},{tb:.2f})"
                
                report.append(f"[COLOR DIFF] Cell {cell_ref} (Row {t_row}, {h}): {desc}")
                    
    return sorted(report)
