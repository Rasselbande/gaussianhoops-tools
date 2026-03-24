"""
export_ranking.py
=================
Liest aus gaussianhoops.db, berechnet Composite-Rank,
Role und Archetype — injiziert alles in gaussianhoops_ncaa_ranking.html.

Ausführen (wann immer DB aktualisiert wurde):
  python3 /Users/phil/Documents/gaussian_hoops/export_ranking.py
"""

import sqlite3
import pandas as pd
import numpy as np
import json
import re
import os
def minmax_scale(arr):
    """Scale a 1-D array to [0, 1]. Returns numpy array."""
    a = np.array(arr, dtype=float)
    mn, mx = np.nanmin(a), np.nanmax(a)
    if mx == mn:
        return np.zeros_like(a)
    return (a - mn) / (mx - mn)

# ── Pfade ──────────────────────────────────────────────────────────────────
_DIR      = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(_DIR, 'gaussianhoops.db')
HTML_PATH = os.path.join(_DIR, 'gaussianhoops_ncaa_ranking.html')
SEASON    = '25-26'
# ──────────────────────────────────────────────────────────────────────────

print("=== Gaussian Hoops — Export Ranking ===\n")

conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()
cur.execute('PRAGMA table_info(players)')
player_cols = [r[1] for r in cur.fetchall()]
has_espn    = 'espn_player_id' in player_cols
espn_sel    = 'p.espn_player_id,' if has_espn else ''

cur.execute('PRAGMA table_info(stats)')
stats_cols  = [r[1] for r in cur.fetchall()]
has_role    = 'role' in stats_cols and 'arch' in stats_cols
role_sel    = 's.role, s.arch,' if has_role else ''
print(f"Role/Arch in DB: {'✅' if has_role else '❌  → migrate_role_arch.py ausführen'}")

df = pd.read_sql(f"""
    SELECT
        p.name, p.height, p.pos, p.pos_group,
        t.abbr AS team, t.full_name, t.conference AS conf, t.tier,
        {espn_sel}
        {role_sel}
        s.season, s.class AS cls,
        s.gp, s.mpg, s.ppg, s.rpg, s.apg, s.spg, s.bpg, s.tov,
        s.fg_pct, s.fg3_pct, s.ft_pct, s.ts_pct, s.efg_pct,
        s.usg_pct, s.per, s.ast_pct, s.tov_pct,
        s.stl_pct, s.blk_pct, s.trb_pct, s.orb_pct, s.ediff,
        s.fg3a_tr, s.ast_to, s.qualified,
        s.ppg_pctile, s.rpg_pctile, s.apg_pctile,
        s.ts_pctile, s.per_pctile, s.usg_pctile,
        s.ast_pct_pctile, s.tov_pct_pctile,
        s.stl_pct_pctile, s.blk_pct_pctile, s.trb_pct_pctile
    FROM stats s
    JOIN players p ON s.player_id = p.id
    JOIN teams   t ON s.team_id   = t.id
    WHERE s.season = '{SEASON}'
    AND s.gp >= 5
""", conn)
conn.close()
print(f"Spieler geladen: {len(df)}")

# ── ROLE ───────────────────────────────────────────────────────────────────
# Aus DB lesen (migrate_role_arch.py muss vorher ausgeführt worden sein)
if not has_role:
    raise RuntimeError("❌ Spalten 'role'/'arch' fehlen in stats-Tabelle.\n   Bitte zuerst ausführen: python3 migrate_role_arch.py")

# ── CONFERENCE MULTIPLIER ──────────────────────────────────────────────────
conf_mult = {
    'Power 5':    1.00,
    'High-Major': 0.88,
    'Mid-Major':  0.76,
    'Low-Major':  0.58,
}
df['conf_mult'] = df['tier'].map(conf_mult).fillna(0.58)

# ── PRODUCTION (35%) — conference-adjusted stats ───────────────────────────
df['ppg_adj'] = df['ppg'] * df['conf_mult']
df['rpg_adj'] = df['rpg'] * df['conf_mult']
df['apg_adj'] = df['apg'] * df['conf_mult']
df['mpg_adj'] = df['mpg'] * df['conf_mult']

df_prod = df[['ppg_adj','rpg_adj','apg_adj','mpg_adj']].fillna(0)
df['prod_score'] = minmax_scale(df_prod.mean(axis=1).values)

# ── EFFICIENCY (30%) ───────────────────────────────────────────────────────
df['ast_to_ratio'] = df['ast_pct'] / (df['tov_pct'] + 0.01)
df_eff = df[['ts_pct','per','ast_to_ratio']].fillna(0)
df['eff_score'] = minmax_scale(df_eff.mean(axis=1).values)

# ── DEFENSE (15%) ─────────────────────────────────────────────────────────
df_def = df[['stl_pct','blk_pct','trb_pct']].fillna(0)
df['def_score'] = minmax_scale(df_def.mean(axis=1).values)

# ── CONTEXT (20%) ─────────────────────────────────────────────────────────
role_val = {'Impact Player': 3, 'Role Player': 2, 'Bench': 1}
tier_val = {'Power 5': 4, 'High-Major': 3, 'Mid-Major': 2, 'Low-Major': 1}

df['role_num'] = df['role'].map(role_val).fillna(1)
df['tier_num'] = df['tier'].map(tier_val).fillna(1)

df['ctx_score'] = minmax_scale(((df['role_num'] + df['tier_num']) / 2).values)

# ── COMBINED SCORE → RANK (0–100) ─────────────────────────────────────────
df['raw_score'] = (
    0.35 * df['prod_score'] +
    0.30 * df['eff_score']  +
    0.15 * df['def_score']  +
    0.20 * df['ctx_score']
)

df['rank'] = (df['raw_score'].rank(pct=True) * 100).round(1)
df['rank'] = df['rank'].fillna(0.0)

print(f"Mit Rank-Score: {(df['rank'] > 0).sum()}")
print("\nScore distribution:")
print(df['rank'].describe().round(1))

# ── Portal: aus portal_list Tabelle laden ──────────────────────────────────
_conn_p = sqlite3.connect(DB_PATH)
_portal_names = pd.read_sql(
    "SELECT name FROM portal_list WHERE season = ?", _conn_p, params=(SEASON,)
)['name'].tolist()
_conn_p.close()
df['portal'] = df['name'].isin(_portal_names).astype(int)
print(f"Portal-Spieler gefunden: {df['portal'].sum()}")

# ── JSON aufbauen ──────────────────────────────────────────────────────────
def safe(v):
    if pd.isna(v): return None
    return v

def build_player(r):
    return {
        'name':   r['name'],
        'team':   r['team'] or '—',
        'cls':    r['cls'] or '—',
        'conf':   r['conf'] or '—',
        'tier':   r['tier'] or '—',
        'pos':    r['pos'] or '—',
        'posGrp': r['pos_group'] or '—',
        'ht':     r['height'] or '—',
        'gp':     float(r['gp']) if pd.notna(r['gp']) else 0,
        'mpg':    round(float(r['mpg']), 1) if pd.notna(r['mpg']) else 0,
        'ppg':    round(float(r['ppg']), 1) if pd.notna(r['ppg']) else 0,
        'rpg':    round(float(r['rpg']), 1) if pd.notna(r['rpg']) else 0,
        'apg':    round(float(r['apg']), 1) if pd.notna(r['apg']) else 0,
        'spg':    round(float(r['spg']), 1) if pd.notna(r['spg']) else 0,
        'bpg':    round(float(r['bpg']), 1) if pd.notna(r['bpg']) else 0,
        'tov':    round(float(r['tov']), 1) if pd.notna(r['tov']) else 0,
        'fgPct':  round(float(r['fg_pct'])*100, 1) if pd.notna(r['fg_pct']) else 0,
        'fg3Pct': round(float(r['fg3_pct'])*100, 1) if pd.notna(r['fg3_pct']) else 0,
        'ftPct':  round(float(r['ft_pct'])*100, 1) if pd.notna(r['ft_pct']) else 0,
        'tsPct':  round(float(r['ts_pct'])*100, 1) if pd.notna(r['ts_pct']) else 0,
        'usg':    round(float(r['usg_pct']), 1) if pd.notna(r['usg_pct']) else 0,
        'per':    round(float(r['per']), 1) if pd.notna(r['per']) else 0,
        'astPct': round(float(r['ast_pct']), 1) if pd.notna(r['ast_pct']) else 0,
        'stlPct': round(float(r['stl_pct']), 1) if pd.notna(r['stl_pct']) else 0,
        'blkPct': round(float(r['blk_pct']), 1) if pd.notna(r['blk_pct']) else 0,
        'tovPct': round(float(r['tov_pct']), 1) if pd.notna(r['tov_pct']) else 0,
        'trbPct': round(float(r['trb_pct']), 1) if pd.notna(r['trb_pct']) else 0,
        'ediff':  round(float(r['ediff']), 1) if pd.notna(r['ediff']) else 0,
        'role':   r['role'],
        'arch':   r['arch'],
        'portal': int(r['portal']),
        'rank':   round(float(r['rank']), 1),
    }

players_list = [build_player(r) for _, r in df.iterrows()]
players_list.sort(key=lambda p: p['rank'], reverse=True)

# Verifikation
top5 = [(p['name'], p['rank'], p['role']) for p in players_list[:5]]
print(f"\nTop 5:")
for name, rank, role in top5:
    print(f"  {rank:5.1f}  {name} ({role})")

# ── In HTML injizieren ─────────────────────────────────────────────────────
with open(HTML_PATH, 'r', encoding='utf-8') as f:
    html = f.read()

players_json = json.dumps(players_list, ensure_ascii=False)
html_new = re.sub(
    r'const RAW\s*=\s*\[.*?\];',
    f'const RAW = {players_json};',
    html,
    flags=re.DOTALL
)

if html_new == html:
    print("\n⚠️  RAW array nicht gefunden/ersetzt!")
else:
    print(f"\n✅ RAW array ersetzt ({len(players_list)} Spieler)")

with open(HTML_PATH, 'w', encoding='utf-8') as f:
    f.write(html_new)

print(f"✅ Gespeichert: {HTML_PATH}")
print(f"   Dateigröße:  {len(html_new)/1024/1024:.1f} MB")

# ── TOP-10 WIDGET exportieren ───────────────────────────────────────────────
WIDGET_PATH = os.path.join(_DIR, 'gaussianhoops_top10_widget.html')

top10 = players_list[:10]
top10_export = []
for i, p in enumerate(top10, 1):
    top10_export.append({
        'rank':  i,
        'name':  p['name'],
        'team':  p['team'],
        'cls':   p['cls'],
        'pos':   p['pos'],
        'ht':    p['ht'],
        'ppg':   p['ppg'],
        'rpg':   p['rpg'],
        'apg':   p['apg'],
        'score': p['rank'],
        'role':  p['role'],
        'conf':  p['conf'],
    })

top10_json = json.dumps(top10_export, ensure_ascii=False)

with open(WIDGET_PATH, 'r', encoding='utf-8') as f:
    widget_html = f.read()

widget_new = re.sub(
    r'const TOP10\s*=\s*\[.*?\];',
    f'const TOP10 = {top10_json};',
    widget_html,
    flags=re.DOTALL
)

with open(WIDGET_PATH, 'w', encoding='utf-8') as f:
    f.write(widget_new)

print(f"✅ Widget gespeichert: {WIDGET_PATH}")
print(f"   Top 3: {top10_export[0]['name']}, {top10_export[1]['name']}, {top10_export[2]['name']}")

print("\nNächste Schritte:")
print("  1. gaussianhoops_ncaa_ranking.html   → GitHub hochladen")
print("  2. gaussianhoops_top10_widget.html   → GitHub hochladen (selber Ordner)")
print("  3. Bei DB-Update: python3 /Users/phil/Documents/gaussian_hoops/export_ranking.py")
