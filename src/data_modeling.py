import polars as pl
from pathlib import Path
import os

BRONZE_PATH = Path().cwd() / "data" / "bronze" / "games.csv"


def create_silver():
    
    old_columns = [
        "AppID", "Name", "Release date", "Estimated owners", "Peak CCU",
        "Required age", "Price", "Discount", "DLC count", "About the game",
        "Supported languages", "Full audio languages", "Reviews", "Header image",
        "Website", "Support url", "Support email", "Windows", "Mac", "Linux",
        "Metacritic score", "Metacritic url", "User score", "Positive", "Negative",
        "Score rank", "Achievements", "Recommendations", "Notes",
        "Average playtime forever", "Average playtime two weeks",
        "Median playtime forever", "Median playtime two weeks",
        "Developers", "Publishers", "Categories", "Genres", "Tags",
        "Screenshots", "Movies"
    ]
    new_columns = [col.lower().replace(" ", "_") for col in old_columns]
    
    df = pl.read_csv(BRONZE_PATH, skip_rows=1, new_columns=new_columns)