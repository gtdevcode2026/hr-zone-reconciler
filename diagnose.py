#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Diagnose why roster users fail to map to the userbase (the 'not found' bug).

Usage (run from the same folder as scripted.py):
    python diagnose.py <userbase_file> <zones_workbook> [--zone NAME]
                       [--roster-tab NAME] [--add-tab NAME]

Read-only: it never writes any file. It replicates scripted.py's cascade match
(Employee Email -> Global Employee ID -> Local Employee ID) and lists every
roster row that matches NOTHING in the userbase, with raw + normalized keys and
the closest near-miss, so the normalization gap is obvious.
"""
import argparse
import os
import sys

import pandas as pd

# Reuse the EXACT loaders/normalizers from the real script.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scripted as S  # noqa: E402

NAME = "Employee Name"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("userbase")
    ap.add_argument("zones")
    ap.add_argument("--zone", default=None)
    ap.add_argument("--roster-tab", default=None)
    ap.add_argument("--add-tab", default=None)
    args = ap.parse_args()

    master = S.load_table(args.userbase)
    roster, add_rows, rname, aname = S.load_zone_tabs(args.zones, args.roster_tab, args.add_tab)
    print("userbase rows: {} | roster tab: {!r} rows: {} | add tab: {!r} rows: {}".format(
        len(master), rname, len(roster), aname, len(add_rows)))
    print("userbase columns:", list(master.columns))
    print("roster   columns:", list(roster.columns))

    # Normalized userbase lookups (same as scripted.py).
    emap = S.build_index_map(master[S.KEY_EMAIL].map(S._norm_email)) if S.KEY_EMAIL in master.columns else {}
    gmap = S.build_index_map(master[S.KEY_ID].map(S._norm_id)) if S.KEY_ID in master.columns else {}
    lmap = S.build_index_map(master[S.LOCAL_ID].map(S._norm_local)) if S.LOCAL_ID in master.columns else {}

    m_email_set = set(emap)
    m_gid_set = set(gmap)
    # gid compared ignoring leading zeros
    gid_no_zeros = {}
    for k in m_gid_set:
        gid_no_zeros.setdefault(k.lstrip("0"), []).append(k)
    # email local-part -> full emails (to catch domain-only differences)
    local_part = {}
    for e in m_email_set:
        local_part.setdefault(e.split("@")[0], []).append(e)

    has_lid = S.LOCAL_ID in roster.columns
    rmask = S.valid_email_mask(roster)
    print("\nroster rows with invalid/blank email (skipped by scripted.py):", int((~rmask).sum()))

    unmatched = []
    for pos in range(len(roster)):
        row = roster.iloc[pos]
        e = S._norm_email(row[S.KEY_EMAIL])
        g = S._norm_id(row[S.KEY_ID])
        l = S._norm_local(row[S.LOCAL_ID]) if has_lid else ""
        if not S.cascade_match(e, g, l, emap, gmap, lmap):
            unmatched.append((pos, row, e, g, l))

    print("UNMATCHED roster rows (match NOTHING in userbase):", len(unmatched))
    print("=" * 78)
    for pos, row, e, g, l in unmatched:
        nm = str(row.get(NAME, "")) if NAME in roster.columns else ""
        print("roster row {}: name={!r}".format(pos, nm))
        print("   raw   email={!r} gid={!r} lid={!r}".format(
            row.get(S.KEY_EMAIL), row.get(S.KEY_ID),
            row.get(S.LOCAL_ID) if has_lid else "(no Local Employee ID column)"))
        print("   norm  email={!r} gid={!r} lid={!r}".format(e, g, l))

        clues = []
        if e and e not in m_email_set:
            lp = e.split("@")[0]
            if lp in local_part:
                clues.append("email LOCAL-PART matches userbase email(s) {} -> DOMAIN differs".format(local_part[lp][:3]))
        if g:
            if g not in m_gid_set:
                gz = g.lstrip("0")
                if gz in gid_no_zeros:
                    clues.append("gid matches userbase id(s) {} after dropping leading zeros".format(gid_no_zeros[gz][:3]))
        if NAME in roster.columns and NAME in master.columns and nm:
            same = master.index[master[NAME].astype(str).str.strip().str.lower() == nm.strip().lower()]
            for idx in list(same)[:2]:
                clues.append("NAME matches userbase row {}: email={!r} gid={!r} lid={!r}".format(
                    idx,
                    master.at[idx, S.KEY_EMAIL] if S.KEY_EMAIL in master.columns else "",
                    master.at[idx, S.KEY_ID] if S.KEY_ID in master.columns else "",
                    master.at[idx, S.LOCAL_ID] if S.LOCAL_ID in master.columns else ""))
        if clues:
            for c in clues:
                print("   CLUE:", c)
        else:
            print("   CLUE: no near-miss found (may genuinely be absent from the userbase)")
        print("-" * 78)


if __name__ == "__main__":
    main()
