#!/usr/bin/env python3
"""
update_pipeline.py — Gaussian Hoops Full Update Pipeline
=========================================================
Runs every step needed after receiving a new season CSV:

  Step 1 — Import CSV → gaussianhoops.db
            (uses TIER from CSV column; falls back to TIER_MAP for blanks/#N/A)
  Step 2 — Recompute all percentiles + qualified flag
  Step 3 — Assign role + arch per player-season
  Step 4 — Rebuild all HTML exports

Usage:
  python3 update_pipeline.py <csv_file> [--season 25-26] [--db gaussianhoops.db]
  python3 update_pipeline.py "NCAA_ALL_25_26 - all.csv"
  python3 update_pipeline.py "NCAA_ALL_24_25 - all.csv" --season 24-25

The script auto-detects the folder it lives in — all DB and HTML paths are
resolved relative to the script, so it works on any machine.
"""

import sqlite3
import csv
import sys
import os
import re
import json
import argparse
import numpy as np
from collections import defaultdict

try:
    import pandas as pd
except ImportError:
    print("❌  pandas not installed — run: pip install pandas")
    sys.exit(1)

# ── Base directory (folder where this script lives) ───────────────────────────
BASE = os.path.dirname(os.path.abspath(__file__))

def p(filename):
    """Resolve a filename relative to the script's folder."""
    return os.path.join(BASE, filename)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — IMPORT CSV → DB
# ══════════════════════════════════════════════════════════════════════════════

TIER_MAP = {
    'ACC':      'Power 5',   'B10':     'Power 5',   'B12':    'Power 5',
    'SEC':      'Power 5',   'Pac-12':  'Power 5',
    'Big East': 'High-Major','AAC':     'High-Major', 'MWC':   'High-Major',
    'WCC':      'High-Major','A-10':    'High-Major', 'MAC':   'High-Major',
    'CUSA':     'Mid-Major', 'SBC':     'Mid-Major',  'MVC':   'Mid-Major',
    'CAA':      'Mid-Major', 'Horizon': 'Mid-Major',  'SoCon': 'Mid-Major',
    'Patriot':  'Mid-Major', 'BSky':    'Mid-Major',  'WAC':   'Mid-Major',
    'OVC':      'Mid-Major', 'Ivy':     'Mid-Major',
    'AEC':      'Low-Major', 'NEC':     'Low-Major',  'MEAC':  'Low-Major',
    'SWAC':     'Low-Major', 'BSouth':  'Low-Major',  'BW':    'Low-Major',
    'Slnd':     'Low-Major',
}

VALID_TIERS = {'Power 5', 'High-Major', 'Mid-Major', 'Low-Major'}

def safe_float(v):
    try:
        f = float(v)
        return None if (f != f) else f
    except:
        return None

def run_import(csv_path, db_path, min_mpg=12, min_gp=5):
    print("\n" + "="*60)
    print("STEP 1 — IMPORT CSV → DB")
    print("="*60)
    print(f"  CSV : {csv_path}")
    print(f"  DB  : {db_path}")

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys=ON")

    with open(csv_path, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))

    # Filter out fully blank/corrupt rows (#N/A in Player field)
    clean_rows = [r for r in rows if r.get('Player','').strip() not in ('', '#N/A')]
    skipped_corrupt = len(rows) - len(clean_rows)
    if skipped_corrupt:
        print(f"  ⚠️  Skipped {skipped_corrupt} corrupt rows (all fields #N/A)")

    league_name = clean_rows[0].get('LEAGUE', 'NCAA').strip()
    c.execute(
        "INSERT OR IGNORE INTO leagues(name,country,level) VALUES(?,'USA','college')",
        (league_name,)
    )
    c.execute("SELECT id FROM leagues WHERE name=?", (league_name,))
    league_id = c.fetchone()[0]

    inserted = skipped = errors = 0
    tier_from_csv = 0
    tier_from_map = 0

    for row in clean_rows:
        # Filter low-minute / low-game players
        if safe_float(row.get('MPG','')) is None or safe_float(row['MPG']) < min_mpg:
            skipped += 1; continue
        if safe_float(row.get('GP','')) is None or safe_float(row['GP']) < min_gp:
            skipped += 1; continue

        name      = row['Player'].strip()
        team_abbr = row['Team'].strip()
        conf      = row['CONFERENCE'].strip()
        season    = row['SEASON'].strip()
        nat       = row['NAT'].strip() if row.get('NAT','').strip() not in ['#N/A',''] else None

        # ── TIER: CSV column takes priority, TIER_MAP as fallback ──────────
        csv_tier = row.get('TIER','').strip()
        if csv_tier in VALID_TIERS:
            tier = csv_tier
            tier_from_csv += 1
        else:
            tier = TIER_MAP.get(conf, 'Low-Major')
            tier_from_map += 1

        # Height
        ht    = row.get('HT','').strip() or None
        ht_cm = None
        if ht and '-' in ht:
            try:
                ft, inch = ht.split('-')
                ht_cm = round(int(ft)*30.48 + int(inch)*2.54, 1)
            except:
                pass

        # Team
        c.execute(
            "INSERT OR IGNORE INTO teams(abbr,full_name,league_id,conference,tier) VALUES(?,?,?,?,?)",
            (team_abbr, row.get('TEAM_NAME','').strip() or None, league_id, conf, tier)
        )
        # Update tier in case it changed (e.g. conference realignment between seasons)
        c.execute(
            "UPDATE teams SET tier=? WHERE abbr=? AND league_id=?",
            (tier, team_abbr, league_id)
        )
        c.execute("SELECT id FROM teams WHERE abbr=? AND league_id=?", (team_abbr, league_id))
        team_id = c.fetchone()[0]

        # Player
        c.execute(
            "INSERT OR IGNORE INTO players(name,nationality,height,height_cm,pos,pos_group) VALUES(?,?,?,?,?,?)",
            (name, nat, ht, ht_cm, row['POS'].strip(), row['POS-GROUP'].strip())
        )
        c.execute("SELECT id FROM players WHERE name=?", (name,))
        result = c.fetchone()
        if not result:
            continue
        player_id = result[0]

        # Stats
        fg3m = safe_float(row.get('3:00 PM') or row.get('3PM',''))
        try:
            c.execute('''INSERT OR REPLACE INTO stats(
                player_id,team_id,season,class,gp,mpg,ppg,fgm,fga,fg_pct,
                fg3m,fg3a,fg3_pct,ftm,fta,ft_pct,orb,drb,rpg,apg,spg,bpg,tov,pf,
                ts_pct,efg_pct,ftr,fg3a_tr,orb_pct,drb_pct,trb_pct,
                ast_pct,tov_pct,stl_pct,blk_pct,usg_pct,
                ortg,drtg,ediff,fic,per,ppr,pps,sq,total_s_pct,ast_to
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (player_id, team_id, season, row['CLASS'].strip(),
             safe_float(row['GP']),   safe_float(row['MPG']),  safe_float(row['PPG']),
             safe_float(row['FGM']),  safe_float(row['FGA']),  safe_float(row['FG%']),
             fg3m, safe_float(row['3PA']), safe_float(row['3P%']),
             safe_float(row['FTM']),  safe_float(row['FTA']),  safe_float(row['FT%']),
             safe_float(row['ORB']),  safe_float(row['DRB']),  safe_float(row['RPG']),
             safe_float(row['APG']),  safe_float(row['SPG']),  safe_float(row['BPG']),
             safe_float(row['TOV']),  safe_float(row['PF']),
             safe_float(row['TS%']),  safe_float(row['eFG%']),
             safe_float(row['FTR']),  safe_float(row['3PATR']),
             safe_float(row['ORB%']), safe_float(row['DRB%']), safe_float(row['TRB%']),
             safe_float(row['AST%']), safe_float(row['TOV%']),
             safe_float(row['STL%']), safe_float(row['BLK%']), safe_float(row['USG%']),
             safe_float(row['ORtg']), safe_float(row['DRtg']), safe_float(row['eDiff']),
             safe_float(row['FIC']),  safe_float(row['PER']),
             safe_float(row['PPR']),  safe_float(row['PPS']),
             safe_float(row['SQ']),   safe_float(row['Total S %']), safe_float(row['AST/TO'])
            ))
            inserted += 1
        except Exception as e:
            errors += 1

    conn.commit()
    conn.close()
    print(f"\n  ✅ Inserted : {inserted}")
    print(f"  ⏭️  Skipped  : {skipped}  (MPG/GP filter)")
    print(f"  ❌ Errors   : {errors}")
    print(f"  📊 TIER source → CSV: {tier_from_csv}  |  fallback TIER_MAP: {tier_from_map}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — RECOMPUTE PERCENTILES
# ══════════════════════════════════════════════════════════════════════════════

MPG_MIN = 12
GP_MIN  = 10

METRICS = [
    ("ppg",     "ppg_pctile",      True),
    ("rpg",     "rpg_pctile",      True),
    ("apg",     "apg_pctile",      True),
    ("spg",     "spg_pctile",      True),
    ("bpg",     "bpg_pctile",      True),
    ("ts_pct",  "ts_pctile",       True),
    ("per",     "per_pctile",      True),
    ("usg_pct", "usg_pctile",      True),
    ("tov_pct", "tov_pct_pctile",  False),
    ("tov",     "tov_pctile",      False),
    ("ast_pct", "ast_pct_pctile",  True),
    ("ast_to",  "ast_to_pctile",   True),
    ("fg3_pct", "fg3_pct_pctile",  True),
    ("fg3a_tr", "fg3a_tr_pctile",  True),
    ("ft_pct",  "ft_pct_pctile",   True),
    ("trb_pct", "trb_pct_pctile",  True),
    ("orb_pct", "orb_pct_pctile",  True),
    ("stl_pct", "stl_pct_pctile",  True),
    ("blk_pct", "blk_pct_pctile",  True),
    ("fg3a",    "fg3a_pctile",     True),
    # New metrics
    ("mpg",     "mpg_pctile",      True),
    ("ortg",    "ortg_pctile",     True),
    ("drtg",    "drtg_pctile",     False),
    ("ftr",     "ftr_pctile",      True),
    ("fg_pct",  "fg_pct_pctile",   True),
    ("orb",     "orb_pctile",      True),
]

NEW_COLS = ["qualified"] + [m[1] for m in METRICS] + ["rpg_vs_avg"]

def percentileofscore(a, score):
    a = np.array([v for v in a if v is not None and not np.isnan(float(v))], dtype=float)
    n = len(a)
    if n == 0: return np.nan
    below = np.sum(a < score)
    equal = np.sum(a == score)
    return 100.0 * (below + 0.5 * equal) / n

def percentile_rank(series, value, higher_is_better=True):
    valid = [v for v in series if v is not None and not np.isnan(float(v))]
    if len(valid) < 5 or value is None:
        return None
    try:
        pct = percentileofscore(valid, float(value))
        return round(pct if higher_is_better else 100 - pct, 1)
    except:
        return None

def run_percentiles(db_path):
    print("\n" + "="*60)
    print("STEP 2 — RECOMPUTE PERCENTILES")
    print("="*60)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Add missing columns idempotently
    existing = [c[1] for c in cur.execute("PRAGMA table_info(stats)").fetchall()]
    added = []
    for col in NEW_COLS:
        if col not in existing:
            dtype = "INTEGER" if col == "qualified" else "REAL"
            cur.execute(f"ALTER TABLE stats ADD COLUMN {col} {dtype}")
            added.append(col)
    conn.commit()
    if added:
        print(f"  Added new columns: {added}")
    else:
        print("  All percentile columns exist — recalculating values")

    stat_cols  = [m[0] for m in METRICS]
    select_str = ", ".join([f"s.{c}" for c in stat_cols])

    rows = cur.execute(f"""
        SELECT s.id, s.gp, s.mpg, {select_str},
               s.rpg, s.season,
               p.pos_group, t.tier
        FROM stats s
        JOIN players p ON s.player_id = p.id
        JOIN teams   t ON s.team_id   = t.id
    """).fetchall()
    print(f"  Total rows: {len(rows)}")

    records = []
    for r in rows:
        rec = dict(r)
        rec['bucket_pos'] = 'FORWARD' if rec['pos_group'] == 'WING' else rec['pos_group']
        gp  = rec['gp']  or 0
        mpg = rec['mpg'] or 0
        rec['qualified'] = 1 if (float(mpg) >= MPG_MIN and float(gp) >= GP_MIN) else 0
        records.append(rec)

    qualified = [r for r in records if r['qualified'] == 1]
    print(f"  Qualified: {len(qualified)}  |  Unqualified: {len(records)-len(qualified)}")

    # Build buckets by pos × tier × season
    buckets = defaultdict(list)
    for r in qualified:
        key = (r['bucket_pos'], r['tier'], r['season'])
        buckets[key].append(r)

    print(f"  Buckets: {len(buckets)}")

    # Compute percentiles
    results = {r['id']: {'qualified': r['qualified']} for r in records}

    for key, bucket in buckets.items():
        for stat_col, pctile_col, higher in METRICS:
            series = [r[stat_col] for r in bucket if r[stat_col] is not None]
            for r in bucket:
                results[r['id']][pctile_col] = percentile_rank(series, r[stat_col], higher)

        # RPG vs avg for this bucket
        rpg_vals = [float(r['rpg']) for r in bucket if r['rpg'] is not None]
        avg = np.mean(rpg_vals) if rpg_vals else None
        for r in bucket:
            if avg and avg != 0 and r['rpg'] is not None:
                results[r['id']]['rpg_vs_avg'] = round(((float(r['rpg']) - avg) / avg) * 100, 1)
            else:
                results[r['id']]['rpg_vs_avg'] = None

    # NULL out unqualified players
    pctile_cols = [m[1] for m in METRICS]
    for r in records:
        if r['qualified'] == 0:
            for col in pctile_cols:
                results[r['id']][col] = None
            results[r['id']]['rpg_vs_avg'] = None

    # Write back to DB
    set_clause  = ", ".join([f"{c} = ?" for c in NEW_COLS])
    update_sql  = f"UPDATE stats SET {set_clause} WHERE id = ?"
    for stats_id, vals in results.items():
        params = ([vals.get('qualified')] +
                  [vals.get(c) for c in pctile_cols] +
                  [vals.get('rpg_vs_avg'), stats_id])
        cur.execute(update_sql, params)

    conn.commit()
    conn.close()
    print(f"  ✅ Updated {len(results)} rows")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — ASSIGN ROLE + ARCH
# ══════════════════════════════════════════════════════════════════════════════

def assign_role(ppg, per, mpg):
    ppg = ppg or 0; per = per or 0; mpg = mpg or 0
    if mpg < 10:                    return 'Bench'
    if ppg >= 14 and per >= 18:     return 'Impact Player'
    if ppg >= 7  or  per >= 13:     return 'Role Player'
    return 'Bench'

def assign_arch(pos_group, fg3a_tr, ast_pct, ast_to, blk_pct, stl_pct, trb_pct):
    pos = (pos_group or '').upper()
    fg3 = fg3a_tr or 0
    ast = ast_pct  or 0
    ast2= ast_to   or 0
    blk = blk_pct  or 0
    stl = stl_pct  or 0
    reb = trb_pct  or 0

    if pos == 'GUARD':
        if ast >= 25 or ast2 >= 1.5: return 'Primary Creator'
        if fg3 >= 35:                return 'Scoring Guard'
        if stl >= 2.5:               return 'Two-Way Guard'
        return 'Scoring Guard'
    elif pos == 'WING':
        if fg3 >= 35:                return 'Stretch Wing'
        if ast >= 20:                return 'Playmaking Wing'
        if stl >= 2.5 or blk >= 2:  return 'Two-Way Wing'
        return 'Stretch Wing'
    elif pos == 'FORWARD':
        if fg3 >= 30:                return 'Stretch Forward'
        if reb >= 15:                return 'Power Forward'
        if ast >= 15:                return 'Playmaking Forward'
        return 'Stretch Forward'
    elif pos == 'BIG':
        if fg3 >= 20:                return 'Stretch Big'
        if blk >= 3:                 return 'Rim Protector'
        if reb >= 18:                return 'Rebounding Big'
        return 'Rim Protector'
    return '—'

def run_role_arch(db_path):
    print("\n" + "="*60)
    print("STEP 3 — ASSIGN ROLE + ARCH")
    print("="*60)

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    # Add columns if missing
    existing = [r[1] for r in cur.execute("PRAGMA table_info(stats)").fetchall()]
    for col in ['role', 'arch']:
        if col not in existing:
            cur.execute(f"ALTER TABLE stats ADD COLUMN {col} TEXT")
            print(f"  Added column '{col}'")

    cur.execute("""
        SELECT s.id,
               s.ppg, s.per, s.mpg,
               s.fg3a_tr, s.ast_pct, s.ast_to, s.blk_pct, s.stl_pct, s.trb_pct,
               p.pos_group
        FROM stats s
        JOIN players p ON s.player_id = p.id
    """)
    rows = cur.fetchall()
    print(f"  Rows loaded: {len(rows)}")

    updates = []
    for row in rows:
        (sid, ppg, per, mpg,
         fg3a_tr, ast_pct, ast_to, blk_pct, stl_pct, trb_pct,
         pos_group) = row
        role = assign_role(ppg, per, mpg)
        arch = assign_arch(pos_group, fg3a_tr, ast_pct, ast_to, blk_pct, stl_pct, trb_pct)
        updates.append((role, arch, sid))

    cur.executemany("UPDATE stats SET role = ?, arch = ? WHERE id = ?", updates)
    conn.commit()

    # Print distribution
    cur.execute("SELECT role, COUNT(*) FROM stats GROUP BY role ORDER BY COUNT(*) DESC")
    print("\n  Role distribution:")
    for role, cnt in cur.fetchall():
        print(f"    {(role or '(empty)'):<20} {cnt}")

    cur.execute("SELECT arch, COUNT(*) FROM stats GROUP BY arch ORDER BY COUNT(*) DESC")
    print("\n  Arch distribution:")
    for arch, cnt in cur.fetchall():
        print(f"    {(arch or '(empty)'):<25} {cnt}")

    conn.close()
    print(f"\n  ✅ Updated {len(updates)} rows")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — REBUILD ALL HTML EXPORTS
# ══════════════════════════════════════════════════════════════════════════════

def run_export(script_name, db_path, season):
    """Run an export script with corrected DB_PATH and SEASON injected at runtime."""
    print(f"\n  → {script_name}")
    script_path = p(script_name)
    if not os.path.exists(script_path):
        print(f"    ⚠️  Not found — skipping")
        return

    with open(script_path, 'r', encoding='utf-8') as f:
        code = f.read()

    # Override hardcoded paths with actual paths relative to BASE
    html_files = {
        'export_ranking.py':         'gaussianhoops_ncaa_ranking.html',
        'export_reb_leaderboard.py': 'gaussianhoops_reb_leaderboard.html',
        'export_3pt_leaderboard.py': 'gaussianhoops_3pt_leaderboard.html',
        'export_compare.py':         'gaussianhoops_compare.html',
        'export_v16.py':             'player_profile_v16.html',
    }
    template_files = {
        'export_compare.py': 'gaussianhoops_compare_template.html',
        'export_v16.py':     'player_profile_v16_template.html',
    }

    ns = {
        '__file__': script_path,
        'DB_PATH':   db_path,
        'SEASON':    season,
    }
    if script_name in html_files:
        ns['HTML_PATH'] = p(html_files[script_name])
        ns['HTML_OUT']  = p(html_files[script_name])
    if script_name in template_files:
        ns['HTML_TEMPLATE'] = p(template_files[script_name])

    # Patch path literals in the code so imports inside scripts resolve correctly
    code = re.sub(r"DB_PATH\s*=\s*['\"].*?['\"]",
                  f"DB_PATH = {db_path!r}", code)
    code = re.sub(r"SEASON\s*=\s*['\"].*?['\"]",
                  f"SEASON = {season!r}", code)
    if script_name in html_files:
        code = re.sub(r"HTML_PATH\s*=\s*['\"].*?['\"]",
                      f"HTML_PATH = {p(html_files[script_name])!r}", code)
        code = re.sub(r"HTML_OUT\s*=\s*['\"].*?['\"]",
                      f"HTML_OUT = {p(html_files[script_name])!r}", code)
    if script_name in template_files:
        code = re.sub(r"HTML_TEMPLATE\s*=\s*['\"].*?['\"]",
                      f"HTML_TEMPLATE = {p(template_files[script_name])!r}", code)

    try:
        exec(compile(code, script_path, 'exec'), ns)
        print(f"    ✅ Done")
    except Exception as e:
        print(f"    ❌ Error: {e}")

def run_exports(db_path, season):
    print("\n" + "="*60)
    print("STEP 4 — REBUILD HTML EXPORTS")
    print("="*60)

    exports = [
        'export_ranking.py',
        'export_reb_leaderboard.py',
        'export_3pt_leaderboard.py',
        'export_compare.py',
        'export_v16.py',
    ]
    for script in exports:
        run_export(script, db_path, season)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Gaussian Hoops — Full DB Update Pipeline'
    )
    parser.add_argument('csv',
        help='Path to the new season CSV (e.g. "NCAA_ALL_25_26 - all.csv")')
    parser.add_argument('--season', default='25-26',
        help='Season tag used in DB and exports (default: 25-26)')
    parser.add_argument('--db', default=None,
        help='Path to gaussianhoops.db (default: auto-detected next to this script)')
    parser.add_argument('--min-mpg', type=float, default=12)
    parser.add_argument('--min-gp',  type=float, default=5)
    parser.add_argument('--skip-import',   action='store_true',
        help='Skip Step 1 (useful if CSV was already imported)')
    parser.add_argument('--skip-exports',  action='store_true',
        help='Skip Step 4 (rebuild HTMLs)')
    args = parser.parse_args()

    # Resolve paths
    csv_path = args.csv if os.path.isabs(args.csv) else os.path.join(BASE, args.csv)
    db_path  = args.db  if args.db else p('gaussianhoops.db')

    if not os.path.exists(csv_path):
        print(f"❌  CSV not found: {csv_path}")
        sys.exit(1)
    if not os.path.exists(db_path):
        print(f"❌  DB not found: {db_path}")
        sys.exit(1)

    print("\n╔══════════════════════════════════════════════════════╗")
    print("║   GAUSSIAN HOOPS — FULL UPDATE PIPELINE              ║")
    print("╚══════════════════════════════════════════════════════╝")
    print(f"  CSV    : {os.path.basename(csv_path)}")
    print(f"  DB     : {os.path.basename(db_path)}")
    print(f"  Season : {args.season}")

    if not args.skip_import:
        run_import(csv_path, db_path, args.min_mpg, args.min_gp)

    run_percentiles(db_path)
    run_role_arch(db_path)

    if not args.skip_exports:
        run_exports(db_path, args.season)

    print("\n" + "="*60)
    print("✅  PIPELINE COMPLETE")
    print("="*60)
    print("\nNext step: upload updated HTML files to GitHub.")
    print("  gaussianhoops_ncaa_ranking.html")
    print("  gaussianhoops_reb_leaderboard.html")
    print("  gaussianhoops_3pt_leaderboard.html")
    print("  gaussianhoops_compare.html")
    print("  player_profile_v16.html")

if __name__ == '__main__':
    main()
