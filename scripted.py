#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Adaptive HR cleaner + per-zone reconciler (DataMart -> userbase).

Run ONCE PER ZONE, manually:

    python scripted.py <datamart_file> <zones_workbook> [--zone NAME]
           [--roster-tab NAME] [--add-tab NAME]
           [--no-clean] [--force-clean] [--chunksize N] [--parallel off|auto|N]

To verify matching without changing anything (read-only), add --diagnose:

    python scripted.py <datamart_file> <zones_workbook> --diagnose [--zone NAME]

It lists roster users that map to NOTHING in the userbase (the cause of any
unexpected "not found in zone"), with raw + normalized keys and near-miss clues.

The pipeline
------------
Step 1  Column titles come from the built-in REQUIRED_COLUMNS list (the "old
        userbase" schema).
Step 2  DataMart provides the DATA; only the REQUIRED_COLUMNS are kept.
Step 3  Rows with blank / '@'-less / 'noemail@' emails are dropped.
        (Steps 1-3 run as the one-time "clean" the FIRST time the DataMart has
        no 'status' column yet; later zone runs skip it so results accumulate.)
Step 4  The cleaned DataMart becomes the new userbase; it is validated against
        the zones workbook. Matching uses a CASCADE key, tried in priority order
        and stopping at the first that hits:
            Employee Email  ->  Global Employee ID  ->  Local Employee ID
        (applied to BOTH zones tabs; matching is global across the userbase).
Step 5  Zones ROSTER tab (the "currently in there" users + an 'Action' column):
          * Action == "ok"                      -> matched userbase row "validated"
          * Action a non-blank value other than -> matched userbase row REMOVED
            "ok" (e.g. "remove from list")
          * Action blank                        -> no-op (row left untouched)
        Userbase rows of this zone that the roster never referenced (and were
        not removed) are KEPT and marked "not found in zone".
Step 6  Zones "add to list" tab -> those users are appended at the BOTTOM of the
        userbase as "newly added" (ones already present are skipped).
Step 7  Duplicate emails are removed from the userbase (keep first).

The userbase (DataMart) file is updated in place, with a timestamped backup
taken before each change.
"""

import argparse
import os
import re
import sys
import shutil
import tempfile
import time
import unicodedata
from typing import List, Optional

import pandas as pd

# -----------------------------
# Config: REQUIRED columns ONLY
# -----------------------------
REQUIRED_COLUMNS: List[str] = [
    "Zone",
    "Country",
    "Global Employee ID",
    "Local Employee ID",
    "Employee Name",
    "Employee Status",
    "Worker Type",
    "Employee Group",
    "Management Level",
    "First Hire Date",
    "Last Hire Date",
    "Position Name",
    "Job Family Group",
    "Job Family",
    "Job Profile Description",
    "ABI Entity 2",
    "Macro Entity Level 2 (Zone)",
    "text before Email",
    "Employee Email",
    "Band 4+",
    "Manager Employee ID Level 01",
    "Manager Name Level 01",
]

# Column / status constants
KEY_ID = "Global Employee ID"
KEY_EMAIL = "Employee Email"
LOCAL_ID = "Local Employee ID"
ZONE_COL = "Zone"
ACTION_COL = "Action"          # roster-tab column; "ok" => validate, other => remove
ACTION_OK = "ok"
STATUS_COLUMN = "status"
STATUS_VALIDATED = "validated"
STATUS_NOT_FOUND = "not found in zone"
STATUS_NEWLY_ADDED = "newly added"

EXCEL_EXTS = {".xlsx", ".xlsm"}


# -----------------------------
# REQUIRED for multiprocessing
# -----------------------------
def chunk_processor(df_chunk: pd.DataFrame) -> pd.DataFrame:
    return filter_required_and_emails(df_chunk)


def identity(df: pd.DataFrame) -> pd.DataFrame:
    return df


def safe_workers(user_value: Optional[str]) -> int:
    if user_value is None or str(user_value).lower() in {"off", "false", "0"}:
        return 0
    if str(user_value).lower() == "auto":
        cpu = os.cpu_count() or 4
        return max(1, cpu - 2)
    try:
        n = int(user_value)
        return max(0, n)
    except Exception:
        cpu = os.cpu_count() or 4
        return max(1, cpu - 2)


# =========================================================
# Step 1: cleaning logic (carried over, unchanged behavior)
# =========================================================
def filter_required_and_emails(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [c for c in REQUIRED_COLUMNS if c in df.columns]
    if not keep_cols:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    df = df[keep_cols].copy()

    if KEY_EMAIL not in df.columns:
        return df.iloc[0:0]

    mask = valid_email_mask(df)
    return df[mask]


def valid_email_mask(df: pd.DataFrame) -> pd.Series:
    """Email-validity rule shared by Step 1 and the zone reconcile step.

    Keeps a row only if the email is non-blank, contains '@', and is not a
    'noemail@' placeholder (case-insensitive)."""
    if KEY_EMAIL not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    email = df[KEY_EMAIL].fillna("").astype(str).str.strip()
    return (
        email.ne("")
        & email.str.contains("@")
        & ~email.str.contains(r"noemail@", case=False)
    )


def find_action_column(df: pd.DataFrame) -> Optional[str]:
    """Return the zone file's action column name (case/space-insensitive), or
    None if the file has no such column."""
    for c in df.columns:
        if str(c).strip().lower() == ACTION_COL.lower():
            return c
    return None


def action_ok_mask(df: pd.DataFrame) -> Optional[pd.Series]:
    """Mask of rows whose action cell equals 'ok' (case-insensitive, trimmed).

    Returns None when the file has no action column, signalling 'no filter'."""
    col = find_action_column(df)
    if col is None:
        return None
    return df[col].fillna("").astype(str).str.strip().str.lower().eq(ACTION_OK)


def process_csv_inplace(
    input_path: str,
    chunksize: int = 200_000,
    parallel: int = 0,
) -> None:
    dirname, basename = os.path.split(input_path)
    root, ext = os.path.splitext(basename)
    backup_path = backup_file(input_path)

    tmp_fd, tmp_path = tempfile.mkstemp(prefix=f"{root}.tmp-", suffix=ext, dir=dirname)
    os.close(tmp_fd)

    read_kwargs = dict(sep=None, engine="python", encoding="utf-8-sig", chunksize=chunksize)

    if parallel > 0:
        from concurrent.futures import ProcessPoolExecutor

        first = True
        in_flight = {}
        next_write_idx = 0
        max_in_flight = max(2, parallel * 3)

        with pd.read_csv(input_path, **read_kwargs) as reader, \
                ProcessPoolExecutor(max_workers=parallel) as pool:

            for idx, df_chunk in enumerate(reader):
                while len(in_flight) >= max_in_flight:
                    done = [k for k, f in in_flight.items() if f.done()]
                    for k in sorted(done):
                        res = in_flight.pop(k).result()
                        if k == next_write_idx:
                            res.to_csv(tmp_path, mode="a", index=False, header=first)
                            first = False
                            next_write_idx += 1
                            while next_write_idx in in_flight and in_flight[next_write_idx].done():
                                res2 = in_flight.pop(next_write_idx).result()
                                res2.to_csv(tmp_path, mode="a", index=False, header=first)
                                first = False
                                next_write_idx += 1
                        else:
                            in_flight[k] = pool.submit(identity, res)

                in_flight[idx] = pool.submit(chunk_processor, df_chunk)

            while next_write_idx in in_flight:
                fut = in_flight.pop(next_write_idx)
                res = fut.result()
                res.to_csv(tmp_path, mode="a", index=False, header=first)
                first = False
                next_write_idx += 1

    else:
        first = True
        for df_chunk in pd.read_csv(input_path, **read_kwargs):
            cleaned = filter_required_and_emails(df_chunk)
            cleaned.to_csv(tmp_path, mode="a", index=False, header=first)
            first = False

    os.replace(tmp_path, input_path)
    print(f"   cleaned CSV written in-place. Backup saved as: {backup_path}")


def process_excel_inplace(input_path: str) -> None:
    backup_path = backup_file(input_path)
    df = pd.read_excel(input_path, engine="openpyxl")
    cleaned = filter_required_and_emails(df)
    write_table_inplace(cleaned, input_path)
    print(f"   cleaned Excel written in-place. Backup saved as: {backup_path}")


def run_clean(master_path: str, chunksize: int, parallel_arg: str) -> None:
    warn_missing_keep_columns(master_path)
    ext = os.path.splitext(master_path)[1].lower()
    if ext == ".csv":
        workers = safe_workers(parallel_arg)
        print(f"   processing CSV in chunks (chunksize={chunksize}, parallel_workers={workers}) ...")
        process_csv_inplace(master_path, chunksize=chunksize, parallel=workers)
    elif ext in EXCEL_EXTS:
        print("   processing Excel ...")
        process_excel_inplace(master_path)
    else:
        print(f"ERROR: Unsupported master file type: {ext}")
        sys.exit(2)


# =========================================================
# Shared file I/O helpers
# =========================================================
def backup_file(path: str) -> str:
    dirname, basename = os.path.split(path)
    root, ext = os.path.splitext(basename)
    ts = time.strftime("%Y%m%d-%H%M%S")
    backup_path = os.path.join(dirname, f"{root}.backup-{ts}{ext}")
    shutil.copy2(path, backup_path)
    return backup_path


def read_header_columns(path: str) -> List[str]:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig", nrows=0)
    elif ext in EXCEL_EXTS:
        df = pd.read_excel(path, engine="openpyxl", nrows=0)
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    return list(df.columns)


def warn_missing_keep_columns(path: str) -> None:
    """Warn if any expected keep-list column is absent from the file, so a
    typo'd / renamed header doesn't silently vanish from the cleaned output."""
    try:
        cols = set(read_header_columns(path))
    except Exception:
        return
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        print("WARNING: these expected columns were NOT found in the master "
              "and will be absent from the cleaned output:")
        for c in missing:
            print(f"   - {c}")


def load_table(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        # Read everything as strings to keep IDs/emails stable for matching.
        return pd.read_csv(
            path, sep=None, engine="python", encoding="utf-8-sig",
            dtype=str, keep_default_na=False,
        )
    elif ext in EXCEL_EXTS:
        return pd.read_excel(path, engine="openpyxl")
    raise ValueError(f"Unsupported file type: {ext}")


def load_zone_tabs(zone_path: str, roster_tab: Optional[str] = None,
                   add_tab: Optional[str] = None):
    """Read the zones workbook as two tabs.

    Returns (roster_df, add_df, roster_name, add_name):
      * roster_df  - the "currently in there" users (first tab, or --roster-tab)
      * add_df     - the "add to list" tab (auto-detected: a tab whose name
                     contains 'add', or --add-tab); empty if none is found
      * roster_name / add_name - the resolved sheet names (add_name is None when
                     there is no add tab)

    A CSV zones file has no tabs: the whole file is the roster and there is no
    add list."""
    ext = os.path.splitext(zone_path)[1].lower()
    if ext == ".csv":
        return load_table(zone_path), pd.DataFrame(), "(csv)", None
    if ext not in EXCEL_EXTS:
        raise ValueError(f"Unsupported zones file type: {ext}")

    xl = pd.ExcelFile(zone_path, engine="openpyxl")
    names = list(xl.sheet_names)
    if not names:
        raise ValueError("Zones workbook has no sheets.")

    # Roster tab: explicit override, else the first sheet.
    roster_name = names[0]
    if roster_tab:
        match = [s for s in names if s.strip().lower() == roster_tab.strip().lower()]
        if not match:
            raise ValueError(f"--roster-tab '{roster_tab}' not found. Tabs: {names}")
        roster_name = match[0]

    # Add tab: explicit override, else a sheet (other than the roster) whose
    # name contains 'add'.
    add_name = None
    if add_tab:
        match = [s for s in names if s.strip().lower() == add_tab.strip().lower()]
        if not match:
            raise ValueError(f"--add-tab '{add_tab}' not found. Tabs: {names}")
        add_name = match[0]
    else:
        for s in names:
            if s != roster_name and "add" in s.strip().lower():
                add_name = s
                break

    roster_df = xl.parse(roster_name)
    add_df = xl.parse(add_name) if add_name else pd.DataFrame()
    return roster_df, add_df, roster_name, add_name


def write_table_inplace(df: pd.DataFrame, path: str) -> None:
    dirname, basename = os.path.split(path)
    root, ext = os.path.splitext(basename)
    ext_l = ext.lower()
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=f"{root}.tmp-", suffix=ext, dir=dirname)
    os.close(tmp_fd)
    if ext_l == ".csv":
        df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    elif ext_l in EXCEL_EXTS:
        df.to_excel(tmp_path, index=False, engine="openpyxl")
    else:
        os.remove(tmp_path)
        raise ValueError(f"Unsupported file type: {ext_l}")
    os.replace(tmp_path, path)


# =========================================================
# Matching helpers
# =========================================================
def _norm_id(v) -> str:
    """Normalize an employee id to a stable string.

    Handles the common Excel quirk where numeric ids load as floats
    (e.g. 12345.0) by stripping the trailing '.0'."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def _norm_email(v) -> str:
    """Normalize an email for matching / dedupe.

    Lower-cased and stripped of characters that are never valid inside an
    address but routinely sneak into exported cells and silently break an exact
    match: a 'mailto:' prefix, surrounding quotes / angle brackets, and ALL
    whitespace and zero-width characters (NBSP, zero-width space/joiner, BOM)
    anywhere in the string. Returns '' for NA. This only removes invalid
    characters, so it can never merge two genuinely different addresses."""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    # NFKC folds full-width / compatibility forms (and NBSP -> space) to ASCII.
    s = unicodedata.normalize("NFKC", str(v)).strip().lower()
    if s.startswith("mailto:"):
        s = s[len("mailto:"):]
    s = s.strip().strip('"\'<>').strip()
    # Remove any whitespace + zero-width characters left anywhere in the string.
    s = re.sub(r"[\s\u200b\u200c\u200d\ufeff]+", "", s)
    return s


def _norm_local(v) -> str:
    """Normalize a Local Employee ID for matching: trimmed + lower-cased.

    Local ids are strings like 'EUR-600101', so (unlike Global IDs) they are not
    run through the float-stripping logic."""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    return str(v).strip().lower()


def build_index_map(norm_series: Optional[pd.Series]) -> dict:
    """Map each non-empty normalized value -> list of master index labels."""
    out: dict = {}
    if norm_series is None:
        return out
    for idx, key in norm_series.items():
        if key:
            out.setdefault(key, []).append(idx)
    return out


def cascade_match(z_email: str, z_gid: str, z_lid: str,
                  emap: dict, gmap: dict, lmap: dict) -> list:
    """Find matching master index labels for one zone row, trying keys in
    priority order and STOPPING at the first tier that yields any hit:
    Employee Email -> Global Employee ID -> Local Employee ID."""
    if z_email and z_email in emap:
        return emap[z_email]
    if z_gid and z_gid in gmap:
        return gmap[z_gid]
    if z_lid and z_lid in lmap:
        return lmap[z_lid]
    return []


# =========================================================
# Step 1 trigger detection
# =========================================================
def is_master_prepared(path: str) -> bool:
    """A master is 'prepared' once it has a 'status' column (i.e. Step 1 + at
    least one reconcile have already run)."""
    try:
        cols = read_header_columns(path)
    except Exception:
        return False
    return STATUS_COLUMN in cols


# =========================================================
# Step 2: reconcile one zone into the master, in place
# =========================================================
def resolve_zone(zone_df: pd.DataFrame, zone_override: Optional[str]) -> str:
    if zone_override:
        return str(zone_override).strip()
    if ZONE_COL not in zone_df.columns:
        raise ValueError("Zone file has no 'Zone' column; pass --zone NAME.")
    vals = zone_df[ZONE_COL].fillna("").astype(str).str.strip()
    uniq = sorted({v for v in vals if v != ""})
    if len(uniq) == 0:
        raise ValueError("Zone file's 'Zone' column is empty; pass --zone NAME.")
    if len(uniq) > 1:
        raise ValueError(f"Zone file contains multiple zones {uniq}; pass --zone NAME.")
    return uniq[0]


def build_appended_rows(new_rows: pd.DataFrame, master_columns, resolved_zone: str) -> pd.DataFrame:
    """Align zone-file 'newly added' rows to the master's columns."""
    if new_rows.empty:
        return pd.DataFrame(columns=master_columns)

    data = {}
    for col in master_columns:
        if col in new_rows.columns:
            data[col] = list(new_rows[col].values)
        else:
            data[col] = [""] * len(new_rows)
    out = pd.DataFrame(data, columns=list(master_columns))

    # Clean up ids for newly-added rows so they don't carry a trailing '.0'.
    if KEY_ID in out.columns:
        out[KEY_ID] = out[KEY_ID].map(_norm_id)

    # Fill the Zone for any new row that didn't carry one.
    if ZONE_COL in out.columns:
        zser = out[ZONE_COL].fillna("").astype(str).str.strip()
        out.loc[zser.eq(""), ZONE_COL] = resolved_zone

    out[STATUS_COLUMN] = STATUS_NEWLY_ADDED
    return out


def reconcile_zone_inplace(master_path: str, zone_path: str, zone_override: Optional[str] = None,
                           roster_tab: Optional[str] = None, add_tab: Optional[str] = None) -> None:
    # 1) Back up the userbase before any change.
    backup_path = backup_file(master_path)

    # 2) Load the userbase (cleaned DataMart); ensure key columns + status. Its
    #    native RangeIndex is kept stable: every match below works on these
    #    labels, and the index is only reset at the very end.
    master = load_table(master_path)
    for col in (KEY_ID, KEY_EMAIL, ZONE_COL):
        if col not in master.columns:
            raise ValueError(f"Userbase is missing required column: '{col}'")
    if STATUS_COLUMN not in master.columns:
        master[STATUS_COLUMN] = ""
    else:
        master[STATUS_COLUMN] = master[STATUS_COLUMN].fillna("")

    # 3) Load the zones workbook: roster tab (current users + Action) and the
    #    separate "add to list" tab. Drop invalid/placeholder emails from each.
    roster, add_rows, roster_name, add_name = load_zone_tabs(zone_path, roster_tab, add_tab)
    for col in (KEY_ID, KEY_EMAIL):
        if col not in roster.columns:
            raise ValueError(f"Zones roster tab '{roster_name}' is missing required column: '{col}'")
    rmask = valid_email_mask(roster)
    dropped_invalid_roster = int((~rmask).sum())
    roster = roster[rmask].reset_index(drop=True)

    # 4) Resolve which zone this workbook is for (from the roster tab).
    resolved_zone = resolve_zone(roster, zone_override)

    # 5) Build cascade lookups from the ORIGINAL userbase (value -> index labels).
    emap = build_index_map(master[KEY_EMAIL].map(_norm_email))
    gmap = build_index_map(master[KEY_ID].map(_norm_id))
    lmap = build_index_map(master[LOCAL_ID].map(_norm_local)) if LOCAL_ID in master.columns else {}

    def match_hits(row, has_lid):
        e = _norm_email(row[KEY_EMAIL])
        g = _norm_id(row[KEY_ID])
        l = _norm_local(row[LOCAL_ID]) if has_lid else ""
        return cascade_match(e, g, l, emap, gmap, lmap)

    # 6) Roster tab -> validate / remove. Action drives the outcome:
    #      "ok"            -> matched userbase row "validated"
    #      non-blank, !=ok -> matched userbase row removed
    #      blank           -> no-op (referenced, left untouched)
    roster_has_lid = LOCAL_ID in roster.columns
    ok_mask = action_ok_mask(roster)
    if ok_mask is None:                       # no Action column -> treat all as ok
        ok_mask = pd.Series([True] * len(roster))
        is_blank = pd.Series([False] * len(roster))
    else:
        action_col = find_action_column(roster)
        action_vals = roster[action_col].fillna("").astype(str).str.strip()
        is_blank = action_vals.eq("")

    validated_idx = set()
    delete_idx = set()
    referenced_idx = set()
    blank_action = 0
    for pos in range(len(roster)):
        hits = match_hits(roster.iloc[pos], roster_has_lid)
        referenced_idx.update(hits)
        if ok_mask.iloc[pos]:
            validated_idx.update(hits)
        elif is_blank.iloc[pos]:
            blank_action += 1
        else:
            delete_idx.update(hits)

    # Removal wins over validation if a userbase row is targeted by both.
    validated_idx -= delete_idx

    # 7) Apply statuses on the ORIGINAL index labels (before any drop/append).
    if validated_idx:
        master.loc[list(validated_idx), STATUS_COLUMN] = STATUS_VALIDATED

    # Userbase rows OF THIS ZONE that the roster never referenced (and were not
    # removed) are kept and flagged "not found in zone".
    zone_norm = str(resolved_zone).strip().lower()
    in_zone = master[ZONE_COL].fillna("").astype(str).str.strip().str.lower() == zone_norm
    not_found_idx = (set(master.index[in_zone]) - referenced_idx) - delete_idx
    if not_found_idx:
        master.loc[list(not_found_idx), STATUS_COLUMN] = STATUS_NOT_FOUND

    # 8) Remove the non-ok matched rows from the userbase.
    if delete_idx:
        master = master.drop(index=list(delete_idx))

    # 9) "add to list" tab -> append users not already in the userbase as
    #    "newly added" (matched against the ORIGINAL userbase via the cascade).
    add_appended = 0
    add_skipped_existing = 0
    dropped_invalid_add = 0
    if not add_rows.empty:
        for col in (KEY_ID, KEY_EMAIL):
            if col not in add_rows.columns:
                raise ValueError(f"Zones add tab '{add_name}' is missing required column: '{col}'")
        amask = valid_email_mask(add_rows)
        dropped_invalid_add = int((~amask).sum())
        add_rows = add_rows[amask].reset_index(drop=True)
        add_has_lid = LOCAL_ID in add_rows.columns
        keep_pos = []
        for pos in range(len(add_rows)):
            if match_hits(add_rows.iloc[pos], add_has_lid):
                add_skipped_existing += 1
            else:
                keep_pos.append(pos)
        to_add = add_rows.iloc[keep_pos] if keep_pos else add_rows.iloc[0:0]
        appended = build_appended_rows(to_add, master.columns, resolved_zone)
        add_appended = int(len(appended))
        master = pd.concat([master, appended], ignore_index=True)
    else:
        master = master.reset_index(drop=True)

    # 10) FINAL step: remove duplicate emails (keep first; blanks are exempt so
    #     they don't all collapse into one row).
    email_norm = master[KEY_EMAIL].map(_norm_email)
    dup_mask = email_norm.duplicated(keep="first") & email_norm.ne("")
    dedupe_removed = int(dup_mask.sum())
    if dedupe_removed:
        master = master[~dup_mask].reset_index(drop=True)

    # 11) Write back in place + summary.
    write_table_inplace(master, master_path)

    add_desc = f"add tab '{add_name}'" if add_name else "no add tab found"
    print(f"OK: reconciled zone '{resolved_zone}' (roster '{roster_name}', {add_desc}).")
    print(f"   Backup saved as: {backup_path}")
    print(f"   validated          : {len(validated_idx)}")
    print(f"   not found in zone  : {len(not_found_idx)}")
    print(f"   removed (Action!=ok): {len(delete_idx)}")
    print(f"   newly added        : {add_appended}")
    if add_skipped_existing:
        print(f"   add-tab users already present (skipped): {add_skipped_existing}")
    if dedupe_removed:
        print(f"   duplicate emails removed: {dedupe_removed}")
    if dropped_invalid_roster:
        print(f"   roster rows skipped (bad email): {dropped_invalid_roster}")
    if dropped_invalid_add:
        print(f"   add-tab rows skipped (bad email): {dropped_invalid_add}")
    if blank_action:
        print(f"   roster rows with blank Action (no-op): {blank_action}")


# =========================================================
# Diagnostics: why do roster users fail to map? (read-only)
# =========================================================
def diagnose_matching(master_path: str, zone_path: str, zone_override: Optional[str] = None,
                      roster_tab: Optional[str] = None, add_tab: Optional[str] = None) -> int:
    """Report roster rows that match NOTHING in the userbase, with raw +
    normalized keys and near-miss clues. Read-only: writes no file. Returns the
    number of unmatched roster rows."""
    name_col = "Employee Name"
    master = load_table(master_path)
    roster, add_rows, rname, aname = load_zone_tabs(zone_path, roster_tab, add_tab)
    print(f"userbase rows: {len(master)} | roster tab: {rname!r} rows: {len(roster)} "
          f"| add tab: {aname!r} rows: {len(add_rows)}")
    print("userbase columns:", list(master.columns))
    print("roster   columns:", list(roster.columns))

    emap = build_index_map(master[KEY_EMAIL].map(_norm_email)) if KEY_EMAIL in master.columns else {}
    gmap = build_index_map(master[KEY_ID].map(_norm_id)) if KEY_ID in master.columns else {}
    lmap = build_index_map(master[LOCAL_ID].map(_norm_local)) if LOCAL_ID in master.columns else {}

    m_email_set = set(emap)
    m_gid_set = set(gmap)
    gid_no_zeros: dict = {}
    for k in m_gid_set:
        gid_no_zeros.setdefault(k.lstrip("0"), []).append(k)
    local_part: dict = {}
    for e in m_email_set:
        local_part.setdefault(e.split("@")[0], []).append(e)

    has_lid = LOCAL_ID in roster.columns
    if KEY_EMAIL in roster.columns:
        rmask = valid_email_mask(roster)
        print("\nroster rows with invalid/blank email (skipped by reconcile):", int((~rmask).sum()))

    unmatched = []
    for pos in range(len(roster)):
        row = roster.iloc[pos]
        e = _norm_email(row[KEY_EMAIL]) if KEY_EMAIL in roster.columns else ""
        g = _norm_id(row[KEY_ID]) if KEY_ID in roster.columns else ""
        l = _norm_local(row[LOCAL_ID]) if has_lid else ""
        if not cascade_match(e, g, l, emap, gmap, lmap):
            unmatched.append((pos, row, e, g, l))

    print("UNMATCHED roster rows (match NOTHING in userbase):", len(unmatched))
    print("=" * 78)
    for pos, row, e, g, l in unmatched:
        nm = str(row.get(name_col, "")) if name_col in roster.columns else ""
        print(f"roster row {pos}: name={nm!r}")
        print("   raw   email={!r} gid={!r} lid={!r}".format(
            row.get(KEY_EMAIL), row.get(KEY_ID),
            row.get(LOCAL_ID) if has_lid else "(no Local Employee ID column)"))
        print(f"   norm  email={e!r} gid={g!r} lid={l!r}")
        clues = []
        if e and e not in m_email_set:
            lp = e.split("@")[0]
            if lp in local_part:
                clues.append("email LOCAL-PART matches userbase email(s) {} -> DOMAIN differs".format(local_part[lp][:3]))
        if g and g not in m_gid_set:
            gz = g.lstrip("0")
            if gz in gid_no_zeros:
                clues.append("gid matches userbase id(s) {} after dropping leading zeros".format(gid_no_zeros[gz][:3]))
        if name_col in roster.columns and name_col in master.columns and nm:
            same = master.index[master[name_col].astype(str).str.strip().str.lower() == nm.strip().lower()]
            for idx in list(same)[:2]:
                clues.append("NAME matches userbase row {}: email={!r} gid={!r} lid={!r}".format(
                    idx,
                    master.at[idx, KEY_EMAIL] if KEY_EMAIL in master.columns else "",
                    master.at[idx, KEY_ID] if KEY_ID in master.columns else "",
                    master.at[idx, LOCAL_ID] if LOCAL_ID in master.columns else ""))
        if clues:
            for c in clues:
                print("   CLUE:", c)
        else:
            print("   CLUE: no near-miss found (may genuinely be absent from the userbase)")
        print("-" * 78)
    return len(unmatched)


# =========================================================
# CLI
# =========================================================
def main():
    ap = argparse.ArgumentParser(
        description="Adaptive HR cleaner + per-zone reconciler (DataMart -> userbase)."
    )
    ap.add_argument("master_file",
                    help="DataMart CSV/Excel. Becomes the userbase; updated in place "
                         "(raw on first run, cleaned + prepared after).")
    ap.add_argument("zone_file",
                    help="The zone's workbook: a roster tab (current users + Action) "
                         "and an 'add to list' tab.")
    ap.add_argument("--zone", default=None,
                    help="Override the zone name (else read from the roster tab's 'Zone' column).")
    ap.add_argument("--roster-tab", default=None,
                    help="Name of the roster tab (default: the workbook's first sheet).")
    ap.add_argument("--add-tab", default=None,
                    help="Name of the 'add to list' tab (default: auto-detect a tab whose "
                         "name contains 'add').")
    ap.add_argument("--diagnose", action="store_true",
                    help="Read-only: report roster users that fail to map to the userbase "
                         "(raw + normalized keys, near-miss clues). No clean, no reconcile, "
                         "no file is written.")
    ap.add_argument("--no-clean", action="store_true",
                    help="Never run Step 1-3 cleaning, even if the DataMart looks raw.")
    ap.add_argument("--force-clean", action="store_true",
                    help="Force cleaning even if the DataMart already has a 'status' column.")
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--parallel", default="off")
    args = ap.parse_args()

    if args.no_clean and args.force_clean:
        print("ERROR: --no-clean and --force-clean are mutually exclusive.")
        sys.exit(2)

    master_path = os.path.abspath(args.master_file)
    zone_path = os.path.abspath(args.zone_file)
    if not os.path.exists(master_path):
        print(f"ERROR: Master not found: {master_path}")
        sys.exit(1)
    if not os.path.exists(zone_path):
        print(f"ERROR: Zone file not found: {zone_path}")
        sys.exit(1)

    if args.diagnose:
        print("-> Diagnose mode (read-only): checking why roster users fail to map ...")
        try:
            diagnose_matching(master_path, zone_path, zone_override=args.zone,
                              roster_tab=args.roster_tab, add_tab=args.add_tab)
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(3)
        return

    prepared = is_master_prepared(master_path)

    # Decide whether Step 1 runs.
    if args.force_clean:
        do_clean = True
    elif args.no_clean:
        do_clean = False
    else:
        do_clean = not prepared

    if do_clean:
        print("-> Step 1: cleaning master (first-time preparation) ...")
        run_clean(master_path, chunksize=args.chunksize, parallel_arg=args.parallel)
    else:
        if not prepared and args.no_clean:
            print("-> Step 1 skipped (--no-clean): master has no 'status' column; "
                  "reconciling it as-is.")
        else:
            print("-> Step 1 skipped (master already prepared).")

    print("-> Step 2: reconciling zones workbook against the userbase ...")
    try:
        reconcile_zone_inplace(master_path, zone_path, zone_override=args.zone,
                               roster_tab=args.roster_tab, add_tab=args.add_tab)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()
