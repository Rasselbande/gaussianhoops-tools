#!/usr/bin/env python3
"""
Gaussian Hoops — Scouting Analytics Engine
============================================
Computes 4 atomic metrics + 2 composite scouting scores.

ATOMIC METRICS
--------------
  AC  — Athleticism Composite      (position-adjusted, percentile 0-100)
  PMI — Playmaking Index           (AST% + AST/TO + USG%, percentile 0-100)
  STI — Shooting Talent Index      (Bayesian 3P% + FT% + TS% + 3PAr, percentile 0-100)
  SEU — Scoring Efficiency at USG  (TS% above expected for usage, percentile 0-100)

COMPOSITE SCORES
----------------
  PVS — Portal Value Score         (immediate impact weight, tier-adjusted)
  DCS — Draft Ceiling Score        (tools / ceiling weight, tier-adjusted, no RS-Sr)

CHANGES v2
----------
  1. RS players: RS- prefix stripped for class ordering; RS-Sr excluded from DCS
  2. Tier adjustment: PVS and DCS multiplied by conference-tier factor after computation
  3. STI upgraded: Bayesian 3P%×0.38 + Bayesian FT%×0.27 + TS%×0.25 + 3PAr×0.10

CHANGES v3
----------
  4. Age multiplier in DCS: DCS × (1 + max(0, (AVG_DRAFT_AGE - player_age) × 0.05))
     - Requires DOB in players table (populated via fetch_dob_wikidata.py)
     - Players without DOB get multiplier = 1.0 (no change)
     - Reference date = Jan 1 of the season's end year (e.g. 2026 for 25-26)

Usage:
  python scouting_engine.py                    # 25-26, preview only
  python scouting_engine.py --season 24-25
  python scouting_engine.py --write-db         # also saves to DB
  python scouting_engine.py --min-gp 10 --min-mpg 15
"""

import os as _os
import sqlite3
import argparse
import datetime
import numpy as np
import pandas as pd

# ── Config ─────────────────────────────────────────────────────────────────────
_HERE   = _os.path.dirname(_os.path.abspath(__file__))
DB_PATH = _os.path.join(_HERE, "gaussianhoops.db")
OUT_CSV = _os.path.join(_HERE, "scouting_output.csv")

DEFAULT_SEASON  = "25-26"
DEFAULT_MIN_GP  = 15
DEFAULT_MIN_MPG = 18

# Bayesian shrinkage constants (how many 'ghost' attempts to add at league mean)
K_FT  = 50    # FT% stabilises quickly
K_3P  = 100   # 3P% needs more shrinkage

# Athleticism weights by position group
AC_WEIGHTS = {
    "GUARD":   {"stl_pct": 0.55, "blk_pct": 0.15, "orb_pct": 0.30},
    "WING":    {"stl_pct": 0.45, "blk_pct": 0.25, "orb_pct": 0.30},
    "FORWARD": {"stl_pct": 0.25, "blk_pct": 0.40, "orb_pct": 0.35},
    "BIG":     {"stl_pct": 0.15, "blk_pct": 0.50, "orb_pct": 0.35},
}

# Composite score weights
PVS_WEIGHTS = {"SEU": 0.35, "PMI": 0.25, "STI": 0.20, "AC": 0.20}
DCS_WEIGHTS = {"STI": 0.30, "AC":  0.28, "PMI": 0.22, "SEU": 0.20}

# ── Tier adjustment factors ─────────────────────────────────────────────────────
# PVS: immediate production depends more on competition level
# DCS: tools/athleticism translate better, so smaller discount
TIER_FACTORS_PVS = {
    "Power 5":   1.00,
    "High-Major": 0.97,
    "Mid-Major":  0.92,
    "Low-Major":  0.85,
}
TIER_FACTORS_DCS = {
    "Power 5":   1.00,
    "High-Major": 0.98,
    "Mid-Major":  0.95,
    "Low-Major":  0.91,
}

# Classes excluded from DCS (too old / not draft-eligible context)
DCS_EXCLUDED_CLASSES = {"RS-Sr", "RS-Jr"}

# Canonical class order (RS prefix stripped for display/sorting)
CLASS_ORDER = {"Fr": 1, "RS-Fr": 1, "So": 2, "RS-So": 2,
               "Jr": 3, "RS-Jr": 3, "Sr": 4, "RS-Sr": 4}

# Age multiplier config
# Average age of a player entering the NBA draft (first round, ~20.5 years)
AVG_DRAFT_AGE = 20.5
# Each year younger than avg adds 5% to DCS; older players capped at 1.0 (no penalty)
AGE_MULT_PER_YEAR = 0.05
# Max boost cap (e.g. 1.30 = 30% max bonus for very young players)
AGE_MULT_CAP = 1.30


# ── Helpers ────────────────────────────────────────────────────────────────────

def series_percentiles(series: pd.Series, higher_is_better=True) -> pd.Series:
    """Vectorised percentile rank for a whole column (0-100, no scipy needed)."""
    s = series.copy().astype(float)
    valid = s.dropna()
    n = len(valid)
    if n < 3:
        return pd.Series(np.nan, index=s.index)
    ranked  = valid.rank(method="average")
    pctiles = (ranked / n * 100).round(1)
    out = pd.Series(np.nan, index=s.index)
    out[valid.index] = pctiles
    if not higher_is_better:
        out[valid.index] = (100 - pctiles).round(1)
    return out


def bayesian_pct(makes, attempts, k, lg_pct) -> pd.Series:
    """Bayesian-adjusted shooting percentage. Shrinks small samples toward league mean."""
    return (makes + k * lg_pct) / (attempts + k)


def strip_rs(class_val: str) -> str:
    """Return base class without RS- prefix (RS-Jr → Jr, Fr → Fr)."""
    if isinstance(class_val, str) and class_val.startswith("RS-"):
        return class_val[3:]
    return class_val


def age_on_date(dob_str, ref_date: datetime.date) -> float:
    """
    Return decimal age (years) on ref_date given a DOB string (YYYY-MM-DD).
    Returns None if dob_str is missing or unparseable.
    """
    if not dob_str or pd.isna(dob_str):
        return None
    try:
        dob = datetime.date.fromisoformat(str(dob_str)[:10])
        delta = ref_date - dob
        return delta.days / 365.25
    except (ValueError, TypeError):
        return None


def age_multiplier(age: float) -> float:
    """
    DCS age multiplier:  1 + max(0, (AVG_DRAFT_AGE - age) × AGE_MULT_PER_YEAR)
    Capped at AGE_MULT_CAP.  Players older than avg draft age get 1.0 (no penalty).
    """
    if age is None:
        return 1.0
    boost = max(0.0, (AVG_DRAFT_AGE - age) * AGE_MULT_PER_YEAR)
    return min(1.0 + boost, AGE_MULT_CAP)


# ── Load data ──────────────────────────────────────────────────────────────────

def load_data(season, min_gp, min_mpg):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"""
        SELECT
            s.id            AS stats_id,
            p.id            AS player_id,
            p.name          AS Player,
            p.pos_group     AS pos_group,
            p.dob           AS dob,
            t.full_name     AS Team,
            t.conference    AS Conference,
            t.tier          AS Tier,
            s.season,
            s.class         AS Class,
            s.gp, s.mpg, s.ppg,
            s.fg3m, s.fg3a, s.fg3_pct,
            s.ftm,  s.fta,  s.ft_pct,
            s.ts_pct,
            s.fg3a_tr,
            s.orb_pct, s.drb_pct, s.trb_pct,
            s.stl_pct, s.blk_pct,
            s.ast_pct, s.tov_pct, s.usg_pct,
            s.ast_to,
            s.ortg, s.drtg, s.ediff
        FROM stats s
        JOIN players p ON s.player_id = p.id
        JOIN teams   t ON s.team_id   = t.id
        WHERE s.season = '{season}'
          AND s.gp  >= {min_gp}
          AND s.mpg >= {min_mpg}
          AND p.pos_group NOT IN ('#N/A', '')
    """, conn)
    conn.close()

    # Add base class column (RS prefix stripped) for display/sorting
    df["Class_base"] = df["Class"].apply(strip_rs)

    return df


# ── Metric 1: Athleticism Composite ───────────────────────────────────────────

def compute_AC(df: pd.DataFrame) -> pd.Series:
    """
    Position-adjusted athleticism composite.
    Raw score = weighted sum of STL%, BLK%, ORB% (weights vary by pos_group).
    Final score = percentile within pos_group (0-100).
    """
    raw = pd.Series(index=df.index, dtype=float)

    for grp, w in AC_WEIGHTS.items():
        mask = df["pos_group"] == grp
        if mask.sum() == 0:
            continue
        raw[mask] = (
            df.loc[mask, "stl_pct"] * w["stl_pct"] +
            df.loc[mask, "blk_pct"] * w["blk_pct"] +
            df.loc[mask, "orb_pct"] * w["orb_pct"]
        )

    ac_pctile = pd.Series(index=df.index, dtype=float)
    for grp in df["pos_group"].unique():
        mask = df["pos_group"] == grp
        ac_pctile[mask] = series_percentiles(raw[mask])

    return ac_pctile.round(1)


# ── Metric 2: Playmaking Index ─────────────────────────────────────────────────

def compute_PMI(df: pd.DataFrame) -> pd.Series:
    """
    Playmaking Index = weighted percentile composite.
      AST%   × 0.40  — creation volume
      AST/TO × 0.40  — decision quality under load
      USG%   × 0.20  — load bearing
    """
    ast_pct_p = series_percentiles(df["ast_pct"])
    ast_to_p  = series_percentiles(df["ast_to"])
    usg_p     = series_percentiles(df["usg_pct"])

    pmi = (ast_pct_p * 0.40 + ast_to_p * 0.40 + usg_p * 0.20)
    return pmi.round(1)


# ── Metric 3: Shooting Talent Index (v2) ──────────────────────────────────────

def compute_STI(df: pd.DataFrame) -> pd.Series:
    """
    Shooting Talent Index v2 — four components:
      Bayesian 3P%  × 0.38  — range & shot quality (most important)
      Bayesian FT%  × 0.27  — pure stroke / mechanics
      TS%           × 0.25  — overall scoring efficiency (captures 2P + mid-range)
      3PAr          × 0.10  — spacing value / shot creation volume bonus

    Bayesian shrinkage pulls small-sample outliers toward league mean.
    TS% normalised to [0,1] before blending (already a ratio, no rescaling needed).
    3PAr normalised by max in pool so it contributes on the same scale.
    """
    lg_ft = df["ft_pct"].mean()
    lg_3p = df["fg3_pct"].mean()

    ft_adj = bayesian_pct(df["ftm"] * df["gp"],   # total FTM
                          df["fta"] * df["gp"],   # total FTA
                          K_FT, lg_ft)

    p3_adj = bayesian_pct(df["fg3m"] * df["gp"],  # total 3PM
                          df["fg3a"] * df["gp"],  # total 3PA
                          K_3P, lg_3p)

    ts  = df["ts_pct"].fillna(df["ts_pct"].median())

    # Normalise 3PAr to [0,1] so it doesn't dominate
    fg3a_tr = df["fg3a_tr"].fillna(0)
    fg3a_tr_norm = fg3a_tr / fg3a_tr.max() if fg3a_tr.max() > 0 else fg3a_tr

    raw_sti = (p3_adj * 0.38 + ft_adj * 0.27 + ts * 0.25 + fg3a_tr_norm * 0.10)
    return series_percentiles(raw_sti).round(1)


# ── Metric 4: Scoring Efficiency at Usage ─────────────────────────────────────

def compute_SEU(df: pd.DataFrame) -> pd.Series:
    """
    SEU = actual TS% minus expected TS% at that USG level (linear regression residual).
    Positive → efficient even when heavily used.
    Negative → declining efficiency under heavy load.
    """
    valid = df[["ts_pct", "usg_pct"]].dropna()
    x = valid["usg_pct"].values
    y = valid["ts_pct"].values
    slope     = (np.cov(x, y)[0, 1]) / np.var(x)
    intercept = y.mean() - slope * x.mean()

    expected_ts = intercept + slope * df["usg_pct"]
    seu_raw     = df["ts_pct"] - expected_ts

    return series_percentiles(seu_raw).round(1)


# ── Composite Scores ───────────────────────────────────────────────────────────

def compute_PVS(df: pd.DataFrame) -> pd.Series:
    """
    Portal Value Score — weights immediate production.
    Tier-adjusted: Low-Major production discounted (×0.85), Power 5 unchanged.
    """
    raw = (df["AC"]  * PVS_WEIGHTS["AC"]  +
           df["PMI"] * PVS_WEIGHTS["PMI"] +
           df["STI"] * PVS_WEIGHTS["STI"] +
           df["SEU"] * PVS_WEIGHTS["SEU"])

    tier_mult = df["Tier"].map(TIER_FACTORS_PVS).fillna(0.85)
    return (raw * tier_mult).round(1)


def compute_DCS(df: pd.DataFrame, ref_date: datetime.date) -> pd.Series:
    """
    Draft Ceiling Score — weights translatable tools & physical upside.
    Tier-adjusted: Low-Major discounted less (×0.91) since tools travel.
    RS-Sr / RS-Jr players set to NaN — not relevant draft ceiling candidates.
    Age multiplier: younger players at same score get a DCS boost (up to ×1.30).
      Formula: DCS × min(AGE_MULT_CAP, 1 + max(0, (AVG_DRAFT_AGE - age) × 0.05))
      Players without DOB receive multiplier = 1.0 (neutral).
    """
    raw = (df["AC"]  * DCS_WEIGHTS["AC"]  +
           df["PMI"] * DCS_WEIGHTS["PMI"] +
           df["STI"] * DCS_WEIGHTS["STI"] +
           df["SEU"] * DCS_WEIGHTS["SEU"])

    tier_mult = df["Tier"].map(TIER_FACTORS_DCS).fillna(0.91)

    # Age multiplier
    ages      = df["dob"].apply(lambda d: age_on_date(d, ref_date))
    age_mults = ages.apply(age_multiplier)

    dcs = (raw * tier_mult * age_mults).round(1)

    # Null out RS-Sr and RS-Jr — not draft ceiling context
    excluded = df["Class"].isin(DCS_EXCLUDED_CLASSES)
    dcs[excluded] = np.nan

    return dcs


# ── Preview output ─────────────────────────────────────────────────────────────

def show_preview(df: pd.DataFrame, ref_date: datetime.date):
    cols_display = ["Player", "pos_group", "Class", "Team", "Tier",
                    "gp", "mpg", "ppg",
                    "AC", "PMI", "STI", "SEU", "PVS", "DCS"]

    def top(col, n=15):
        return df.dropna(subset=[col]).nlargest(n, col)[cols_display].to_string(index=False)

    print("\n" + "="*80)
    print("SCOUTING ANALYTICS ENGINE v2 — PREVIEW")
    print("="*80)

    print(f"\n{'─'*40}")
    print("TOP 15 — ATHLETICISM COMPOSITE (AC)")
    print(f"{'─'*40}")
    print(top("AC"))

    print(f"\n{'─'*40}")
    print("TOP 15 — PLAYMAKING INDEX (PMI)")
    print(f"{'─'*40}")
    print(top("PMI"))

    print(f"\n{'─'*40}")
    print("TOP 15 — SHOOTING TALENT INDEX (STI)")
    print(f"{'─'*40}")
    print(top("STI"))

    print(f"\n{'─'*40}")
    print("TOP 15 — SCORING EFFICIENCY AT USAGE (SEU)")
    print(f"{'─'*40}")
    print(top("SEU"))

    print(f"\n{'─'*40}")
    print("TOP 20 — PORTAL VALUE SCORE (PVS)  [tier-adjusted]")
    print(f"{'─'*40}")
    print(top("PVS", 20))

    print(f"\n{'─'*40}")
    print("TOP 20 — DRAFT CEILING SCORE (DCS)  [tier-adjusted, no RS-Sr/RS-Jr]")
    print(f"{'─'*40}")
    print(top("DCS", 20))

    # Distribution summary
    print(f"\n{'─'*40}")
    print("METRIC DISTRIBUTIONS (all qualified players)")
    print(f"{'─'*40}")
    print(df[["AC","PMI","STI","SEU","PVS","DCS"]].describe().round(1).to_string())

    # Class breakdown — use base class (RS prefix stripped)
    print(f"\n{'─'*40}")
    print("DCS BY CLASS (avg — draft context, RS prefix stripped)")
    print(f"{'─'*40}")
    print(df.groupby("Class_base")[["DCS","STI","AC","PMI"]]
            .mean().round(1)
            .sort_values("DCS", ascending=False)
            .to_string())

    # Tier impact summary
    print(f"\n{'─'*40}")
    print("TIER ADJUSTMENT IMPACT (avg PVS pre vs post)")
    print(f"{'─'*40}")
    for tier, factor in sorted(TIER_FACTORS_PVS.items(), key=lambda x: -x[1]):
        mask = df["Tier"] == tier
        if mask.sum() == 0:
            continue
        avg_pvs = df.loc[mask, "PVS"].mean()
        n = mask.sum()
        print(f"  {tier:<15}  factor={factor:.2f}  avg PVS={avg_pvs:.1f}  n={n}")

    # Age multiplier summary
    print(f"\n{'─'*40}")
    print("AGE MULTIPLIER COVERAGE (DCS players with DOB)")
    print(f"{'─'*40}")
    dcs_df = df.dropna(subset=["DCS", "dob"])
    if len(dcs_df) > 0:
        ages = dcs_df["dob"].apply(lambda d: age_on_date(d, ref_date))
        mults = ages.apply(age_multiplier)
        print(f"  Players with DOB in DCS pool : {len(dcs_df)}")
        print(f"  Avg age multiplier           : {mults.mean():.3f}")
        print(f"  Max multiplier (youngest)    : {mults.max():.3f}")
        print(f"  Players at cap (×{AGE_MULT_CAP})        : {(mults >= AGE_MULT_CAP).sum()}")
        # Show top boosted players
        boosted = dcs_df.copy()
        boosted["age"]      = ages
        boosted["age_mult"] = mults
        top_boosted = (boosted.nlargest(5, "age_mult")
                              [["Player", "Class", "Team", "age", "age_mult", "DCS"]])
        print(f"\n  Top 5 youngest (most boosted):")
        print(top_boosted.to_string(index=False))
    else:
        print("  No DOB data available — run fetch_dob_wikidata.py first")


# ── DB write ───────────────────────────────────────────────────────────────────

def compute_adj_dcs(df: pd.DataFrame) -> pd.Series:
    """
    Adj DCS — production-based DCS with proportional class/age/AC bonus.
    Bonus scales with base DCS so low producers don't get unfairly inflated.
    """
    CLASS_MAX = {'Fr': 22, 'RS-Fr': 14, 'So': 10, 'RS-So': 5, 'Jr': 0, 'Sr': -12}
    AGE_MAX   = {18: 18, 19: 11, 20: 5, 21: 0, 22: -7}

    def _row(r):
        dcs = r["DCS"] if not pd.isna(r["DCS"]) else 0
        cls = str(r.get("Class_base", "") or "")
        age = r.get("age_years", np.nan)
        ac  = r["AC"] if not pd.isna(r["AC"]) else 50

        # Class bonus
        max_b = next((v for k, v in CLASS_MAX.items() if cls.startswith(k)), 0)
        cb = max_b * (dcs / 100)

        # Age bonus
        age_key = int(min(round(age), 22)) if not np.isnan(age) else 21
        ab = AGE_MAX.get(age_key, -10 if (not np.isnan(age) and age > 22) else 0) * (dcs / 100)

        # AC premium — only elite athleticism (AC > 75)
        acb = (ac - 75) * 0.12 * (dcs / 100) if ac > 75 else 0

        return round(min(100, max(0, dcs + cb + ab + acb)), 1)

    return df.apply(_row, axis=1)


def compute_sps(df: pd.DataFrame) -> pd.Series:
    """
    SPS — Scout Profile Score: measures NBA CEILING, not college production.
    Built around the three pillars scouts use for lottery picks:
      1. YOUTH       — development runway (multiplicative, not additive)
      2. SCORING     — raw PPG volume (can he create his own shot?)
      3. POSITION    — NBA positional premium (wings most valued)
    AC and PMI contribute to production base but youth/position do the heavy lifting.
    """
    YOUTH_MULT = {18: 1.60, 19: 1.38, 20: 1.14, 21: 1.00, 22: 0.80, 23: 0.65}
    POS_MULT   = {'WING': 1.18, 'FORWARD': 1.08, 'GUARD': 1.00, 'BIG': 0.93}

    def _row(r):
        ppg = r.get("ppg", 0) or 0
        ac  = r["AC"]  if not pd.isna(r["AC"])  else 50
        pmi = r["PMI"] if not pd.isna(r["PMI"]) else 50
        age = r.get("age_years", np.nan)
        pos = str(r.get("pos_group", "") or "")

        scoring    = min(100, (ppg / 25) * 100)
        production = scoring * 0.45 + ac * 0.28 + pmi * 0.27

        age_key = int(min(max(round(age), 18), 23)) if not np.isnan(age) else 21
        y_mult  = YOUTH_MULT.get(age_key, 0.55)
        p_mult  = POS_MULT.get(pos, 1.0)

        return round(min(100, production * y_mult * p_mult), 1)

    return df.apply(_row, axis=1)


def write_to_db(df: pd.DataFrame):
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    new_cols = ["AC", "PMI", "STI", "SEU", "PVS", "DCS", "adj_dcs", "sps"]
    for col in new_cols:
        try:
            cur.execute(f"ALTER TABLE stats ADD COLUMN {col.lower()} REAL")
            print(f"  Added column: {col.lower()}")
        except Exception:
            pass  # column already exists

    updated = 0
    for _, row in df.iterrows():
        dcs_val     = None if pd.isna(row["DCS"])     else row["DCS"]
        adj_dcs_val = None if pd.isna(row["ADJ_DCS"]) else row["ADJ_DCS"]
        sps_val     = None if pd.isna(row["SPS"])     else row["SPS"]
        cur.execute("""
            UPDATE stats
            SET ac=?, pmi=?, sti=?, seu=?, pvs=?, dcs=?, adj_dcs=?, sps=?
            WHERE id=?
        """, (row["AC"], row["PMI"], row["STI"], row["SEU"],
              row["PVS"], dcs_val, adj_dcs_val, sps_val, row["stats_id"]))
        updated += 1

    conn.commit()
    conn.close()
    print(f"\n  ✅ Updated {updated} rows in stats table.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Gaussian Hoops Scouting Engine v2")
    parser.add_argument("--season",   default=DEFAULT_SEASON)
    parser.add_argument("--min-gp",   type=int, default=DEFAULT_MIN_GP)
    parser.add_argument("--min-mpg",  type=float, default=DEFAULT_MIN_MPG)
    parser.add_argument("--write-db", action="store_true",
                        help="Write metrics back to gaussianhoops.db")
    args = parser.parse_args()

    print(f"\nLoading season={args.season}  min_gp={args.min_gp}  min_mpg={args.min_mpg} ...")
    df = load_data(args.season, args.min_gp, args.min_mpg)
    print(f"Qualified players: {len(df)}")

    # Reference date for age calculation = Jan 1 of season end year
    # e.g. "25-26" → 2026-01-01
    try:
        end_year = 2000 + int(args.season.split("-")[1])
    except (ValueError, IndexError):
        end_year = datetime.date.today().year
    ref_date = datetime.date(end_year, 1, 1)

    dob_count = df["dob"].notna().sum()
    print(f"Players with DOB: {dob_count}/{len(df)}  (age ref date: {ref_date})")

    print("Computing metrics...")
    df["AC"]       = compute_AC(df)
    df["PMI"]      = compute_PMI(df)
    df["STI"]      = compute_STI(df)
    df["SEU"]      = compute_SEU(df)
    df["PVS"]      = compute_PVS(df)
    df["DCS"]      = compute_DCS(df, ref_date)
    # Pre-compute age_years once — shared by ADJ_DCS and SPS
    df["age_years"] = df["dob"].apply(lambda d: age_on_date(d, ref_date))
    df["ADJ_DCS"]  = compute_adj_dcs(df)
    df["SPS"]      = compute_sps(df)

    show_preview(df, ref_date)

    # Save CSV
    out_cols = ["Player", "pos_group", "Class", "Class_base", "Team", "Conference", "Tier",
                "season", "gp", "mpg", "ppg",
                "stl_pct", "blk_pct", "orb_pct",
                "ast_pct", "ast_to", "usg_pct",
                "ft_pct", "fg3_pct", "ts_pct", "fg3a_tr",
                "AC", "PMI", "STI", "SEU", "PVS", "DCS", "ADJ_DCS", "SPS"]
    df[out_cols].sort_values("PVS", ascending=False).to_csv(OUT_CSV, index=False)
    print(f"\n📄 CSV saved → {OUT_CSV}")

    if args.write_db:
        print("\nWriting to DB...")
        write_to_db(df)
    else:
        print("\n💡 Run with --write-db to persist metrics to gaussianhoops.db")


if __name__ == "__main__":
    main()
