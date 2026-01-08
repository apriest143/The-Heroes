import pandas as pd
import requests
from bs4 import BeautifulSoup

SEASON = 2011
BASE = "https://www.sports-reference.com"

from bs4 import Comment

def get_tournament_games(season):
    base = "https://www.sports-reference.com"
    url = f"{base}/cbb/postseason/{season}-ncaa-boxscores.html"

    print(f"Fetching tournament boxscore index: {url}")
    soup = BeautifulSoup(requests.get(url).text, "lxml")

    game_links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/cbb/boxscores/"):
            game_links.append(base + href)

    print(f"Found {len(game_links)} men's tournament games")
    return sorted(set(game_links))

def scrape_game_boxscore(url):
    try:
        tables = pd.read_html(url)
        player_tables = []

        for table in tables:
            if "Player" in table.columns:
                table = table[table["Player"] != "Team Totals"]
                table["game_url"] = url
                player_tables.append(table)

        return player_tables

    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
        return []


def build_tournament_player_stats(season):
    games = get_tournament_games(season)
    all_players = []

    for i, game in enumerate(games, 1):
        print(f"[{i}/{len(games)}] Scraping {game}")
        tables = scrape_game_boxscore(game)
        all_players.extend(tables)

    if not all_players:
        print("No tournament data found.")
        return

    df = pd.concat(all_players, ignore_index=True)
    df.to_csv(f"data/tournament_player_boxscores_{season}.csv", index=False)
    print("Saved CSV!")


if __name__ == "__main__":
    build_tournament_player_stats(SEASON)