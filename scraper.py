"""
Yahoo Fantasy Baseball – Roto Standings Scraper
==============================================
Captures daily roto category stats and points
and appends them to a Power BI–ready fact table.
"""

# ── Imports ─────────────────────────────────────────────────────────────
import json
import time
from datetime import date
from pathlib import Path

import pandas as pd
from requests_oauthlib import OAuth2Session


# ── CONFIGURATION ────────────────────────────────────────────────────────
LEAGUE_ID = "35457"
SEASON = 2026

YAHOO_CLIENT_ID     = "dj0yJmk9U1pHSzJmaDMwaVFYJmQ9WVdrOVV6bERNWHBtWkVVbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PWIw"
YAHOO_CLIENT_SECRET = "b9c4bcdc638f5ea1ad84bddefa40b62438e2aeea"

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

TOKEN_FILE = Path(".yahoo_token.json")
MASTER_CSV = DATA_DIR / "standings_category_fact.csv"


# ── STAT ID → DISPLAY NAME ───────────────────────────────────────────────
# Maps Yahoo's numeric stat IDs to human-readable category names.
# These are the standard MLB roto categories Yahoo uses.
STAT_NAMES = {
    "1":  "GP",    # Games Played
    "2":  "AB",    # At Bats
    "3":  "AVG",   # Batting Average
    "4":  "OBP",   # On Base Percentage
    "5":  "SLG",   # Slugging Percentage
    "6":  "OPS",   # OPS
    "7":  "R",     # Runs
    "8":  "H",     # Hits
    "9":  "1B",    # Singles
    "10": "2B",    # Doubles
    "11": "3B",    # Triples
    "12": "HR",    # Home Runs
    "13": "RBI",   # RBI
    "14": "BB",    # Walks (batting)
    "15": "IBB",   # Intentional Walks
    "16": "SB",    # Stolen Bases
    "17": "CS",    # Caught Stealing
    "18": "HBP",   # Hit By Pitch
    "19": "SAC",   # Sacrifice Flies
    "20": "SF",    # Sacrifice Hits
    "21": "SO",    # Strikeouts (batting)
    "22": "GIDP",  # Grounded Into DP
    "23": "TB",    # Total Bases
    "24": "XBH",   # Extra Base Hits
    "25": "NSB",   # Net Stolen Bases
    "26": "ERA",   # ERA
    "27": "WHIP",  # WHIP
    "28": "W",     # Wins
    "29": "L",     # Losses
    "30": "GS",    # Games Started
    "31": "CG",    # Complete Games
    "32": "SHO",   # Shutouts
    "33": "SV",    # Saves
    "34": "OBA",   # Opponent BA
    "35": "IP",    # Innings Pitched (legacy)
    "36": "H_p",   # Hits Allowed
    "37": "ER",    # Earned Runs
    "38": "HR_p",  # HR Allowed
    "39": "BB_p",  # Walks Allowed
    "40": "SO_p",  # Strikeouts (pitching) legacy
    "41": "BBI",   # BB/9
    "42": "K",     # Strikeouts (pitching)
    "43": "KBB",   # K/BB ratio
    "44": "NSV",   # Net Saves
    "45": "HLD",   # Holds
    "46": "BSV",   # Blown Saves
    "47": "SVO",   # Save Opportunities
    "48": "K9",    # K/9
    "49": "K_BB",  # K-BB
    "50": "IP2",   # Innings Pitched
    "51": "WL",    # W+L
    "55": "QS",    # Quality Starts
    "57": "NH",    # No Hitters
    "60": "SVH",   # Saves + Holds
    "83": "XBA",   # Expected BA
    "89": "SV2",   # Saves (alternate)
}


# ── HELPERS ──────────────────────────────────────────────────────────────
def to_float(value):
    """Safely convert Yahoo values to float."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def stat_name(stat_id):
    """Look up a human-readable name for a stat ID."""
    return STAT_NAMES.get(str(stat_id), f"stat_{stat_id}")


# ── AUTHENTICATION ───────────────────────────────────────────────────────
def get_oauth_session():
    """Creates or refreshes a Yahoo OAuth2 session."""

    redirect_uri           = "https://localhost:8080"
    authorization_base_url = "https://api.login.yahoo.com/oauth2/request_auth"
    token_url              = "https://api.login.yahoo.com/oauth2/get_token"

    oauth = OAuth2Session(
        YAHOO_CLIENT_ID,
        redirect_uri=redirect_uri,
        scope=["openid", "fspt-r"],
    )

    if TOKEN_FILE.exists():
        token = json.loads(TOKEN_FILE.read_text())
        oauth.token = token
        if token.get("expires_at", 0) < time.time() + 60:
            print("Refreshing expired token...")
            token = oauth.refresh_token(
                token_url,
                client_id=YAHOO_CLIENT_ID,
                client_secret=YAHOO_CLIENT_SECRET,
            )
            TOKEN_FILE.write_text(json.dumps(token))
        return oauth

    # First-time authorization
    auth_url, _ = oauth.authorization_url(authorization_base_url)
    print("\n" + "="*60)
    print("FIRST-TIME AUTHORIZATION REQUIRED")
    print("="*60)
    print("\nStep 1: Open this URL in your browser:\n")
    print(f"  {auth_url}\n")
    print("Step 2: Log in and click Allow.")
    print("Step 3: Your browser will show a broken/blank page.")
    print("        That is normal! Copy the ENTIRE URL from your")
    print("        browser address bar and paste it below.\n")

    redirect_response = input("Paste the full redirect URL here: ").strip()
    token = oauth.fetch_token(
        token_url,
        authorization_response=redirect_response,
        client_secret=YAHOO_CLIENT_SECRET,
    )
    TOKEN_FILE.write_text(json.dumps(token))
    print("\n✓ Authorization complete — token saved for future runs.\n")
    return oauth


# ── DATA EXTRACTION ──────────────────────────────────────────────────────
def get_standings(league_id: str) -> list[dict]:
    """
    Fetches roto standings from Yahoo Fantasy API.
    Uses the standings endpoint which contains both
    team_stats (raw values) and team_points (roto points).
    """
    oauth    = get_oauth_session()
    base     = "https://fantasysports.yahooapis.com/fantasy/v2"
    game_key = f"mlb.l.{league_id}"

    print(f"Fetching standings for league {league_id}...")
    resp = oauth.get(f"{base}/league/{game_key}/standings?format=json")
    resp.raise_for_status()
    data = resp.json()

    teams = (data["fantasy_content"]["league"][1]
                  ["standings"][0]["teams"])

    snapshot_date = date.today().isoformat()
    rows = []

    for i in range(teams["count"]):
        t    = teams[str(i)]["team"]
        meta = t[0]
        body = t[1]

        # Team name
        name = next(x["name"] for x in meta if "name" in x)

        # Rank info — may or may not be present early in season
        tstands = body.get("team_standings", {})
        outcome = tstands.get("outcome_totals", {})
        rank    = tstands.get("rank")
        wins    = int(outcome.get("wins",   0)) if outcome else None
        losses  = int(outcome.get("losses", 0)) if outcome else None
        ties    = int(outcome.get("ties",   0)) if outcome else None
        pct     = to_float(outcome.get("percentage")) if outcome else None
        gb      = tstands.get("games_back")

        # Raw category stats
        stat_list   = body.get("team_stats",  {}).get("stats", [])
        points_list = body.get("team_points", {}).get("stats", [])

        # Build lookup: stat_id → roto points
        points_by_id = {}
        for p in points_list:
            s = p["stat"]
            points_by_id[str(s["stat_id"])] = to_float(s.get("value"))

        if stat_list:
            for entry in stat_list:
                s       = entry["stat"]
                sid     = str(s["stat_id"])
                rows.append({
                    "snapshot_date": snapshot_date,
                    "league_id":     league_id,
                    "team_name":     name,
                    "rank":          rank,
                    "wins":          wins,
                    "losses":        losses,
                    "ties":          ties,
                    "pct":           pct,
                    "gb":            gb,
                    "stat_id":       sid,
                    "category":      stat_name(sid),
                    "raw_value":     to_float(s.get("value")),
                    "roto_points":   points_by_id.get(sid),
                })
        else:
            # Pre-season: no stats yet, save a placeholder row
            rows.append({
                "snapshot_date": snapshot_date,
                "league_id":     league_id,
                "team_name":     name,
                "rank":          rank,
                "wins":          wins,
                "losses":        losses,
                "ties":          ties,
                "pct":           pct,
                "gb":            gb,
                "stat_id":       None,
                "category":      None,
                "raw_value":     None,
                "roto_points":   None,
            })

    print(f"✓ Retrieved {len(rows)} rows across {teams['count']} teams")
    return rows


# ── PERSISTENCE ───────────────────────────────────────────────────────────
def save_snapshot(rows: list[dict]):
    """Append today's rows to the master CSV (Power BI safe)."""

    if not rows:
        print("No rows returned — nothing to save")
        return pd.DataFrame()

    df        = pd.DataFrame(rows)
    temp_file = MASTER_CSV.with_suffix(".tmp")

    # Only read existing file if it exists AND has actual content
    existing_ok = (
        MASTER_CSV.exists()
        and MASTER_CSV.stat().st_size > 0
    )

    if existing_ok:
        try:
            existing = pd.read_csv(MASTER_CSV)
            if existing.empty:
                combined = df
            else:
                combined = pd.concat([existing, df], ignore_index=True)
        except Exception as e:
            print(f"Warning: Could not read master CSV ({e}) — starting fresh")
            combined = df
    else:
        combined = df

    combined.to_csv(temp_file, index=False)
    temp_file.replace(MASTER_CSV)

    daily_file = DATA_DIR / f"standings_{date.today().isoformat()}.csv"
    df.to_csv(daily_file, index=False)

    print(f"✓ Master CSV updated → {MASTER_CSV}")
    print(f"✓ Daily backup saved → {daily_file}")
    return df


# ── MAIN ─────────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print(" Yahoo Fantasy Baseball – Daily Roto Snapshot")
    print(f" {date.today()}")
    print("="*60 + "\n")

    rows = get_standings(LEAGUE_ID)
    df   = save_snapshot(rows)

    print("\n── Sample Output ───────────────────────────")
    cols = ["team_name", "rank", "category", "raw_value", "roto_points"]
    print(df[cols].head(20).to_string(index=False))
    print()


if __name__ == "__main__":
    main()
