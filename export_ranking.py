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
from scipy import stats as scipy_stats

# ── Pfade ──────────────────────────────────────────────────────────────────
DB_PATH   = '/Users/phil/Documents/gaussian_hoops/gaussianhoops.db'
HTML_PATH = '/Users/phil/Documents/gaussian_hoops/gaussianhoops_ncaa_ranking.html'
SEASON    = '25-26'
# ──────────────────────────────────────────────────────────────────────────

print("=== Gaussian Hoops — Export Ranking ===\n")

conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()
cur.execute('PRAGMA table_info(players)')
player_cols = [r[1] for r in cur.fetchall()]
has_espn    = 'espn_player_id' in player_cols
espn_sel    = 'p.espn_player_id,' if has_espn else ''

df = pd.read_sql(f"""
    SELECT
        p.name, p.height, p.pos, p.pos_group,
        t.abbr AS team, t.full_name, t.conference AS conf, t.tier,
        {espn_sel}
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

# ── Composite Rank (0–100) ─────────────────────────────────────────────────
# Gewichtete Summe aus den wichtigsten Percentile-Spalten
WEIGHTS = {
    'per_pctile':      0.25,
    'ppg_pctile':      0.20,
    'ts_pctile':       0.15,
    'tov_pct_pctile':  0.12,
    'ast_pct_pctile':  0.10,
    'trb_pct_pctile':  0.08,
    'stl_pct_pctile':  0.05,
    'blk_pct_pctile':  0.05,
}

pctile_cols = list(WEIGHTS.keys())
df_pct = df[pctile_cols].copy()

# Nur Spieler mit ≥ 5 Percentile-Werten bewerten
has_enough = df_pct.notna().sum(axis=1) >= 5

composite = pd.Series(0.0, index=df.index)
total_w   = pd.Series(0.0, index=df.index)
for col, w in WEIGHTS.items():
    mask = df_pct[col].notna()
    composite[mask] += df_pct[col][mask] * w
    total_w[mask]   += w

composite = np.where(total_w > 0, composite / total_w, np.nan)
composite = np.where(has_enough, composite, np.nan)

# Auf 0–100 normieren (Percentile-Rank innerhalb aller Spieler)
valid_mask = ~np.isnan(composite)
ranks      = np.full(len(composite), np.nan)
if valid_mask.sum() > 0:
    pct_ranks = scipy_stats.rankdata(composite[valid_mask], method='average')
    pct_ranks = (pct_ranks - 1) / (valid_mask.sum() - 1) * 100
    ranks[valid_mask] = np.round(pct_ranks, 1)

df['rank'] = ranks
df['rank'] = df['rank'].fillna(0.0)

print(f"Mit Rank-Score: {(df['rank'] > 0).sum()}")

# ── Role (basierend auf Rank) ──────────────────────────────────────────────
def assign_role(rank):
    if pd.isna(rank) or rank == 0: return 'Bench'
    if rank >= 75: return 'Impact Player'
    if rank >= 40: return 'Role Player'
    return 'Bench'

df['role'] = df['rank'].apply(assign_role)

# ── Archetype (basierend auf Pos + Stats) ─────────────────────────────────
def assign_arch(row):
    pos  = (row.get('pos_group') or '').upper()
    fg3  = row.get('fg3a_tr') or 0      # 3-point attempt rate
    ast  = row.get('ast_pct') or 0      # assist pct
    ast2 = row.get('ast_to') or 0       # ast/to ratio
    blk  = row.get('blk_pct') or 0      # block pct
    stl  = row.get('stl_pct') or 0      # steal pct
    reb  = row.get('trb_pct') or 0      # rebound pct

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
    return 'Role Player'

df['arch'] = df.apply(assign_arch, axis=1)

# ── Portal (nicht in DB → 0) ───────────────────────────────────────────────
df['portal'] = 0

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
print("\nNächste Schritte:")
print("  1. gaussianhoops_ncaa_ranking.html in GitHub hochladen")
print("  2. Bei DB-Update: python3 export_ranking.py → erneut hochladen")
