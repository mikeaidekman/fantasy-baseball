"""
Yahoo Fantasy Baseball – Team Lookup Table Builder
===================================================
Pulls the current team roster from Yahoo API and saves
dim_team.csv mapping team_id to team_name.

Run this:
  - Once at the start of the season
  - Any time a team renames themselves
  - It overwrites dim_team.csv each time so it stays current

Usage:
  python build_dim_team.py
"""

import json
import time
from pathlib import Path

import pandas as pd
from requests_oauthlib import OAuth2Session


# ── CONFIGURATION ────────────────────────────────────────────────────────
LEAGUE_ID           = "35457"

YAHOO_CLIENT_ID     = "dj0yJmk9U1pHSzJmaDMwaVFYJmQ9WVdrOVV6bERNWHBtWkVVbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PWIw"
YAHOO_CLIENT_SECRET = "b9c4bcdc638f5ea1ad84bddefa40b62438e2aeea"

DATA_DIR     = Path("data")
DATA_DIR.mkdir(exist_ok=True)

TOKEN_FILE   = Path(".yahoo_token.json")
DIM_TEAM_CSV = DATA_DIR / "dim_team.csv"


# ── AUTHENTICATION ───────────────────────────────────────────────────────
def get_oauth_session():
    """Creates or refreshes a Yahoo OAuth2 session."""

    redirect_uri           = "http://localhost:8080"
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

    auth_url, _ = oauth.authorization_url(authorization_base_url)
    print("\n" + "="*60)
    print("FIRST-TIME AUTHORIZATION REQUIRED")
    print("="*60)
    print("\nStep 1: Open this URL in your browser:\n")
    print(f"  {auth_url}\n")
    print("Step 2: Log in and click Allow.")
    print("Step 3: Copy the ENTIRE URL from your browser address bar.\n")

    redirect_response = input("Paste the full redirect URL here: ").strip()
    token = oauth.fetch_token(
        token_url,
        authorization_response=redirect_response,
        client_secret=YAHOO_CLIENT_SECRET,
    )
    TOKEN_FILE.write_text(json.dumps(token))
    print("\n✓ Authorization complete\n")
    return oauth


# ── DATA EXTRACTION ──────────────────────────────────────────────────────
def build_dim_team(league_id: str):
    """Fetches team roster and saves dim_team.csv."""

    oauth    = get_oauth_session()
    base     = "https://fantasysports.yahooapis.com/fantasy/v2"
    game_key = f"mlb.l.{league_id}"

    print(f"Fetching teams for league {league_id}...")
    resp = oauth.get(f"{base}/league/{game_key}/teams?format=json")
    resp.raise_for_status()
    data = resp.json()

    raw_teams = data["fantasy_content"]["league"][1]["teams"]

    rows = []
    for i in range(raw_teams["count"]):
        t         = raw_teams[str(i)]["team"][0]
        team_id   = next(x["team_id"] for x in t if "team_id" in x)
        team_name = next(x["name"]    for x in t if "name"    in x)
        team_url  = next((x["url"]    for x in t if "url"     in x), None)

        rows.append({
            "team_id":   team_id,
            "team_name": team_name,
            "team_url":  team_url,
        })

    df = pd.DataFrame(rows)
    df.to_csv(DIM_TEAM_CSV, index=False)

    print(f"\n✓ Saved {len(df)} teams → {DIM_TEAM_CSV}\n")
    print(df.to_string(index=False))
    print()
    return df


# ── MAIN ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "="*50)
    print(" Yahoo Fantasy Baseball – Team Lookup Builder")
    print("="*50 + "\n")

    build_dim_team(LEAGUE_ID)
