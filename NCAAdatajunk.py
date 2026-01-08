### Oh man I think this is gonna be a bit rough
### gonna begin with scraping sports-reference for basic player stats

### gotta have this installed btw pip install requests beautifulsoup4 pandas lxml

### Much later we will connect to some other data sources for play-by-play, metadata, and advanced stats
### For now lets just get gordon Haywards basic stats from his seasons at butler

import pandas as pd
import time
import os
import requests

# -----------------------------
# CONFIG
# -----------------------------
SEASON = 2011
DATA_DIR = "data"
RATE_LIMIT_SLEEP = 1.0  # be polite to Sports-Reference

os.makedirs(DATA_DIR, exist_ok=True)

###Grabbing Players
def get_players_for_season(season: int) -> pd.DataFrame:
    """
    Pulls the list of all players who appeared in a given season.
    """
    url = f"https://www.sports-reference.com/cbb/seasons/{season}-players.html"
    print(f"Fetching player index: {url}")

    tables = pd.read_html(url)
    players_df = tables[0]

    # Drop repeated header rows
    players_df = players_df[players_df['Player'] != 'Player']

    return players_df


#### Extract player pages
from bs4 import BeautifulSoup, Comment

def get_player_urls(season: int) -> list:
    """
    Returns a list of full URLs to player pages for a season.
    Handles Sports-Reference commented tables.
    """
    url = f"https://www.sports-reference.com/cbb/seasons/{season}-players.html"
    print(f"Fetching player index: {url}")

    html = requests.get(url).text
    soup = BeautifulSoup(html, "lxml")

    player_urls = []

    # 1. Find all HTML comments
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))

    for comment in comments:
        comment_soup = BeautifulSoup(comment, "lxml")
        table = comment_soup.find("table", id="players")

        if table is None:
            continue

        for row in table.tbody.find_all("tr"):
            player_cell = row.find("th", {"data-stat": "player"})
            if player_cell and player_cell.a:
                href = player_cell.a["href"]
                full_url = "https://www.sports-reference.com" + href
                player_urls.append(full_url)

        break  # players table found, no need to keep searching

    print(f"Found {len(player_urls)} player URLs")
    return player_urls

### Pulling Single player game logs
def get_player_gamelog(player_url: str, season: int) -> pd.DataFrame:
    """
    Pulls a player's game log for a given season.
    """
    gamelog_url = f"{player_url}/gamelog/{season}"
    try:
        tables = pd.read_html(gamelog_url)
        df = tables[0]
        df["player_url"] = player_url
        return df
    except Exception:
        return pd.DataFrame()
    
### Build the tournament dataset
def build_tournament_player_stats(season: int):
    player_urls = get_player_urls(season)
    print(f"Found {len(player_urls)} players")

    all_rows = []

    for i, player_url in enumerate(player_urls):
        print(f"[{i+1}/{len(player_urls)}] {player_url}")
        df = get_player_gamelog(player_url, season)

        if df.empty:
            continue

        # Drop repeated headers
        df = df[df['Date'] != 'Date']

        # NCAA Tournament games are labeled in the Notes column
        if 'Notes' in df.columns:
            tourney_df = df[df['Notes'].str.contains("NCAA", na=False)]
        else:
            continue

        if not tourney_df.empty:
            all_rows.append(tourney_df)

        time.sleep(RATE_LIMIT_SLEEP)

    if not all_rows:
        print("No tournament data found.")
        return

    final_df = pd.concat(all_rows, ignore_index=True)
    out_path = f"{DATA_DIR}/player_tournament_gamelogs_{season}.csv"
    final_df.to_csv(out_path, index=False)
    print(f"Saved tournament player stats to {out_path}")


### RUNIT 
if __name__ == "__main__":
    build_tournament_player_stats(SEASON)