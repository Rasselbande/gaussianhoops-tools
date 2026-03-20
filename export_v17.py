"""
export_v17.py
=============
Liest aus gaussianhoops.db und injiziert alle Spielerdaten
in player_profile_v17_template.html → player_profile_v17.html

Neu in v17: AC, PMI, STI, SEU + berechnete Percentiles

Ausführen (wann immer DB aktualisiert wurde):
  python3 /Users/phil/Documents/gaussian_hoops/export_v17.py
"""

import sqlite3
import pandas as pd
import json
import re

# ── Pfade ──────────────────────────────────────────────────────────────────
DB_PATH       = '/Users/phil/Documents/gaussian_hoops/gaussianhoops.db'
HTML_TEMPLATE = '/Users/phil/Documents/gaussian_hoops/player_profile_v17_template.html'
HTML_OUT      = '/Users/phil/Documents/gaussian_hoops/player_profile_v17.html'
SEASON        = '25-26'
# ──────────────────────────────────────────────────────────────────────────

print("=== Gaussian Hoops — Export v17 ===\n")

# ── DB laden ───────────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

cur.execute('PRAGMA table_info(players)')
player_cols = [r[1] for r in cur.fetchall()]
has_espn    = 'espn_player_id' in player_cols
espn_select = 'p.espn_player_id,' if has_espn else ''
print(f"ESPN IDs in DB: {'✅' if has_espn else '❌  → update_db_espn_headshots.py ausführen'}")

cur.execute('PRAGMA table_info(stats)')
stats_cols  = [r[1] for r in cur.fetchall()]
has_role    = 'role' in stats_cols and 'arch' in stats_cols
role_select = 's.role, s.arch,' if has_role else ''
print(f"Role/Arch in DB: {'✅' if has_role else '❌  → migrate_role_arch.py ausführen'}")

df = pd.read_sql(f"""
    SELECT
        p.name, p.nationality, p.height, p.pos_group,
        t.abbr, t.full_name, t.conference, t.tier,
        {espn_select}
        {role_select}
        s.season, s.class, s.gp, s.mpg,
        s.ppg, s.rpg, s.apg, s.spg, s.bpg, s.tov, s.pf,
        s.fgm, s.fga, s.fg_pct,
        s.fg3m, s.fg3a, s.fg3_pct, s.fg3a_tr,
        s.ftm, s.fta, s.ft_pct, s.ftr,
        s.ts_pct, s.efg_pct, s.orb_pct, s.drb_pct, s.trb_pct,
        s.ast_pct, s.tov_pct, s.stl_pct, s.blk_pct,
        s.usg_pct, s.per, s.ast_to, s.qualified,
        s.ortg, s.drtg, s.orb,
        s.ppg_pctile, s.rpg_pctile, s.apg_pctile,
        s.spg_pctile, s.bpg_pctile,
        s.ts_pctile, s.per_pctile, s.usg_pctile,
        s.ast_pct_pctile, s.tov_pct_pctile, s.ast_to_pctile,
        s.fg3_pct_pctile, s.fg3a_tr_pctile, s.fg3a_pctile,
        s.ft_pct_pctile, s.trb_pct_pctile, s.orb_pct_pctile,
        s.stl_pct_pctile, s.blk_pct_pctile,
        s.tov_pctile,
        s.mpg_pctile, s.ortg_pctile, s.drtg_pctile,
        s.ftr_pctile, s.fg_pct_pctile, s.orb_pctile,
        s.rpg_vs_avg,
        s.ac, s.pmi, s.sti, s.seu, s.pvs, s.dcs,
        meas.height_shoes_in, meas.wingspan_in,
        meas.standing_reach_in, meas.wingspan_ratio,
        meas.meas_season, meas.meas_event
    FROM stats s
    JOIN players p ON s.player_id = p.id
    JOIN teams   t ON s.team_id   = t.id
    LEFT JOIN (
        SELECT m1.player_id,
               m1.height_shoes_in, m1.wingspan_in,
               m1.standing_reach_in, m1.wingspan_ratio,
               m1.season AS meas_season, m1.event AS meas_event
        FROM measurements m1
        INNER JOIN (
            SELECT player_id, MAX(season) AS max_season
            FROM measurements WHERE player_id IS NOT NULL GROUP BY player_id
        ) m2 ON m1.player_id = m2.player_id AND m1.season = m2.max_season
        WHERE m1.id = (
            SELECT id FROM measurements m3
            WHERE m3.player_id = m1.player_id AND m3.season = m1.season
            ORDER BY CASE m3.event WHEN 'Combine' THEN 1
                                   WHEN 'BWoB'    THEN 2
                                   WHEN 'PIT'     THEN 3 ELSE 4 END
            LIMIT 1
        )
    ) meas ON meas.player_id = p.id
    WHERE s.season = '{SEASON}'
    ORDER BY s.ppg DESC
""", conn)
# ── Seasons history (all seasons for players in current season) ─────────────
df_hist = pd.read_sql(f"""
    SELECT p.name, s.season, s.gp, s.mpg,
           s.ppg, s.rpg, s.apg, s.spg, s.bpg, s.tov, s.pf,
           s.fgm, s.fga, s.fg_pct, s.fg3m, s.fg3a, s.fg3_pct, s.fg3a_tr,
           s.ftm, s.fta, s.ft_pct, s.ftr,
           s.ts_pct, s.orb_pct, s.trb_pct, s.ast_pct, s.tov_pct,
           s.stl_pct, s.blk_pct, s.usg_pct, s.per, s.ortg, s.drtg, s.orb
    FROM stats s
    JOIN players p ON s.player_id = p.id
    WHERE p.name IN (
        SELECT DISTINCT p2.name FROM stats s2
        JOIN players p2 ON s2.player_id = p2.id
        WHERE s2.season = '{SEASON}'
    )
    ORDER BY p.name, s.season ASC
""", conn)
conn.close()

print(f"Spieler geladen:  {len(df)}")
print(f"Mit Percentiles:  {df['ppg_pctile'].notna().sum()}")
if has_role:
    print(f"Mit Role/Arch:    {df['role'].notna().sum()}")

# ── AC/PMI/STI/SEU Percentiles (berechnet on-the-fly) ──────────────────────
# Higher is better for all six (SEU positive = overperforms expectations)
for col in ['ac', 'pmi', 'sti', 'seu', 'pvs', 'dcs']:
    df[f'{col}_pctile'] = df[col].rank(pct=True, na_option='keep') * 100
print(f"Mit AC/PMI/STI/SEU: {df['ac'].notna().sum()} | PVS/DCS: {df['pvs'].notna().sum()}")
print(f"Mit Messungen:      {df['wingspan_in'].notna().sum()}")

# ── Konvertierung ──────────────────────────────────────────────────────────
def safe(val):
    return None if pd.isna(val) else val

def safe_int(val):
    return None if pd.isna(val) else int(val)

def row_to_player(r):
    return {
        'name':           r['name'],
        'full_name':      r['full_name'],
        'pos_group':      r['pos_group'],
        'tier':           r['tier'],
        'conference':     r['conference'],
        'season':         r['season'],
        'class':          safe(r['class']),
        'nationality':    safe(r['nationality']),
        'height':         safe(r['height']),
        'gp':             safe_int(r['gp']),
        'mpg':            safe(r['mpg']),
        'qualified':      int(r['qualified']) if pd.notna(r['qualified']) else 0,
        'player_id':      safe_int(r['espn_player_id']) if has_espn else None,
        'ppg':            safe(r['ppg']),
        'rpg':            safe(r['rpg']),
        'apg':            safe(r['apg']),
        'spg':            safe(r['spg']),
        'bpg':            safe(r['bpg']),
        'tov':            safe(r['tov']),
        'pf':             safe(r['pf']),
        'fgm':            safe(r['fgm']),
        'fga':            safe(r['fga']),
        'fg_pct':         safe(r['fg_pct']),
        'fg3m':           safe(r['fg3m']),
        'ftm':            safe(r['ftm']),
        'fta':            safe(r['fta']),
        'ts_pct':         safe(r['ts_pct']),
        'efg_pct':        safe(r['efg_pct']),
        'orb_pct':        safe(r['orb_pct']),
        'trb_pct':        safe(r['trb_pct']),
        'ast_pct':        safe(r['ast_pct']),
        'tov_pct':        safe(r['tov_pct']),
        'stl_pct':        safe(r['stl_pct']),
        'blk_pct':        safe(r['blk_pct']),
        'usg_pct':        safe(r['usg_pct']),
        'per':            safe(r['per']),
        'fg3_pct':        safe(r['fg3_pct']),
        'fg3a':           safe(r['fg3a']),
        'fg3a_tr':        safe(r['fg3a_tr']),
        'ft_pct':         safe(r['ft_pct']),
        'ftr':            safe(r['ftr']),
        'ast_to':         safe(r['ast_to']),
        'ortg':           safe(r['ortg']),
        'drtg':           safe(r['drtg']),
        'orb':            safe(r['orb']),
        'ppg_pctile':     safe(r['ppg_pctile']),
        'rpg_pctile':     safe(r['rpg_pctile']),
        'apg_pctile':     safe(r['apg_pctile']),
        'spg_pctile':     safe(r['spg_pctile']),
        'bpg_pctile':     safe(r['bpg_pctile']),
        'ts_pctile':      safe(r['ts_pctile']),
        'per_pctile':     safe(r['per_pctile']),
        'usg_pctile':     safe(r['usg_pctile']),
        'ast_pct_pctile': safe(r['ast_pct_pctile']),
        'tov_pct_pctile': safe(r['tov_pct_pctile']),
        'ast_to_pctile':  safe(r['ast_to_pctile']),
        'fg3_pct_pctile': safe(r['fg3_pct_pctile']),
        'fg3a_tr_pctile': safe(r['fg3a_tr_pctile']),
        'fg3a_pctile':    safe(r['fg3a_pctile']),
        'ft_pct_pctile':  safe(r['ft_pct_pctile']),
        'trb_pct_pctile': safe(r['trb_pct_pctile']),
        'orb_pct_pctile': safe(r['orb_pct_pctile']),
        'stl_pct_pctile': safe(r['stl_pct_pctile']),
        'blk_pct_pctile': safe(r['blk_pct_pctile']),
        'tov_pctile':     safe(r['tov_pctile']),
        'mpg_pctile':     safe(r['mpg_pctile']),
        'ortg_pctile':    safe(r['ortg_pctile']),
        'drtg_pctile':    safe(r['drtg_pctile']),
        'ftr_pctile':     safe(r['ftr_pctile']),
        'fg_pct_pctile':  safe(r['fg_pct_pctile']),
        'orb_pctile':     safe(r['orb_pctile']),
        'rpg_vs_avg':     safe(r['rpg_vs_avg']),
        'role':           r['role'] if has_role else None,
        'arch':           r['arch'] if has_role else None,
        # ── Scouting KPIs ──────────────────────────────────────────────────
        'ac':             safe(r['ac']),
        'pmi':            safe(r['pmi']),
        'sti':            safe(r['sti']),
        'seu':            safe(r['seu']),
        'pvs':            safe(r['pvs']),
        'dcs':            safe(r['dcs']),
        'ac_pctile':      safe(r['ac_pctile']),
        'pmi_pctile':     safe(r['pmi_pctile']),
        'sti_pctile':     safe(r['sti_pctile']),
        'seu_pctile':     safe(r['seu_pctile']),
        'pvs_pctile':     safe(r['pvs_pctile']),
        'dcs_pctile':     safe(r['dcs_pctile']),
        # ── Physical Measurements ──────────────────────────────────────────
        'height_shoes_in':    safe(r['height_shoes_in']),
        'wingspan_in':        safe(r['wingspan_in']),
        'standing_reach_in':  safe(r['standing_reach_in']),
        'wingspan_ratio':     safe(r['wingspan_ratio']),
        'meas_season':        safe_int(r['meas_season']),
        'meas_event':         safe(r['meas_event']),
    }

players_list = [row_to_player(r) for _, r in df.iterrows()]

# ── Build seasons history map ───────────────────────────────────────────────
seasons_map = {}
for _, r in df_hist.iterrows():
    name = r['name']
    if name not in seasons_map:
        seasons_map[name] = []
    seasons_map[name].append({
        'season':  r['season'],
        'gp':      None if pd.isna(r['gp']) else int(r['gp']),
        'mpg':     safe(r['mpg']),   'ppg':     safe(r['ppg']),
        'fgm':     safe(r['fgm']),   'fga':     safe(r['fga']),   'fg_pct':  safe(r['fg_pct']),
        'fg3m':    safe(r['fg3m']),  'fg3a':    safe(r['fg3a']),  'fg3_pct': safe(r['fg3_pct']),
        'ftm':     safe(r['ftm']),   'fta':     safe(r['fta']),   'ft_pct':  safe(r['ft_pct']),
        'rpg':     safe(r['rpg']),   'apg':     safe(r['apg']),   'spg':     safe(r['spg']),
        'bpg':     safe(r['bpg']),   'pf':      safe(r['pf']),    'tov':     safe(r['tov']),
        'ts_pct':  safe(r['ts_pct']),'fg3a_tr': safe(r['fg3a_tr']),
        'orb_pct': safe(r['orb_pct']),'trb_pct':safe(r['trb_pct']),
        'ast_pct': safe(r['ast_pct']),'tov_pct':safe(r['tov_pct']),
        'stl_pct': safe(r['stl_pct']),'blk_pct':safe(r['blk_pct']),
        'usg_pct': safe(r['usg_pct']),'per':    safe(r['per']),
        'ftr':     safe(r['ftr']),   'ortg':    safe(r['ortg']),  'drtg':    safe(r['drtg']),
        'orb':     safe(r['orb']),
    })

# Add seasons (last 5, oldest first = newest at bottom of table)
for p in players_list:
    p['seasons'] = seasons_map.get(p['name'], [])[-5:]

print(f"Mit Headshot:     {sum(1 for p in players_list if p['player_id'])}")

# Verifikation
boozer = next((p for p in players_list if 'Boozer' in p['name']), None)
if boozer:
    print(f"\nCameron Boozer: ppg={boozer['ppg']}, tier={boozer['tier']}, player_id={boozer['player_id']}")

# ── Inject in Template ─────────────────────────────────────────────────────
with open(HTML_TEMPLATE, 'r', encoding='utf-8') as f:
    html = f.read()

players_json = json.dumps(players_list, ensure_ascii=False, indent=None)

html_new = re.sub(
    r'const PLAYERS\s*=\s*\[\];',
    f'const PLAYERS = {players_json};',
    html
)

if html_new == html:
    print("\n⚠️  PLAYERS array nicht ersetzt — Template korrekt?")
else:
    print("✅ PLAYERS array injiziert")

# ── Speichern ──────────────────────────────────────────────────────────────
with open(HTML_OUT, 'w', encoding='utf-8') as f:
    f.write(html_new)

print(f"\n✅ Gespeichert: {HTML_OUT}")
print(f"   Dateigröße:  {len(html_new) / 1024 / 1024:.1f} MB")
print("\nNächste Schritte:")
print("  1. player_profile_v17.html in GitHub hochladen")
print("  2. Bei DB-Update: python3 /Users/phil/Documents/gaussian_hoops/export_v17.py")
