# hr-zone-reconciler

Adaptive HR cleaner + per-zone reconciler. Turns a raw **DataMart** export into a
maintained **userbase**, reconciled against a per-zone workbook.

## Usage

```bash
python scripted.py <DataMart> <zones_workbook> --zone <ZoneName>
```

Run once per zone. The DataMart file is updated in place (a timestamped backup is
taken before each change). The zones workbook is read-only.

### Options

| Flag | Purpose |
|------|---------|
| `--zone NAME` | Override the zone name (else read from the roster tab's `Zone` column). |
| `--roster-tab NAME` | Name of the roster tab (default: the workbook's first sheet). |
| `--add-tab NAME` | Name of the "add to list" tab (default: auto-detect a tab whose name contains `add`). |
| `--no-clean` | Never run the first-time clean, even if the DataMart looks raw. |
| `--force-clean` | Force the clean even if the DataMart already has a `status` column. |
| `--chunksize N` | CSV read chunk size (default 200000). |
| `--parallel off\|auto\|N` | Parallel workers for CSV cleaning. |

## Pipeline

1. Column titles come from the built-in `REQUIRED_COLUMNS` list.
2. The DataMart supplies the data; only those columns are kept.
3. Rows with blank / `@`-less / `noemail@` emails are dropped.
   *(Steps 1–3 run as a one-time clean the first time the DataMart has no `status` column.)*
4. The cleaned DataMart becomes the userbase and is validated against the zones workbook.
   Matching uses a cascade key, stopping at the first that hits:
   **Employee Email → Global Employee ID → Local Employee ID** (applied to both tabs).
5. **Roster tab** (`Action` column): `ok` → `validated`; any other non-blank value
   (e.g. `remove from list`) → the matched userbase row is **removed**; blank → no-op.
   Userbase rows of the zone that the roster never referenced are kept as `not found in zone`.
6. **"add to list" tab** → those users are appended at the bottom as `newly added`
   (anyone already present is skipped).
7. Duplicate emails are removed from the userbase (keep first).

## Notes

- Matching is global across the userbase, so a person is found even if they sit in a
  different zone (cross-zone matches still validate).
- Only the first worksheet of the DataMart is read; the output is a single sheet.
- No data files are committed (see `.gitignore`).
