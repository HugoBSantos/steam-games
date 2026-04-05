import polars as pl
import psycopg
from pathlib import Path
import os

BRONZE_PATH = Path().cwd() / "data" / "bronze" / "games.csv"


def clean_and_split_str(column_name: str) -> pl.Expr:
    html_regex = r"&amp;lt;.*?&amp;gt;|<.*?>|&amp;[a-z]+;"
    escape_regex = r"\\r|\\n|\\t"
    paren_regex = r"\(.*?\)"
    bbcode_regex = r"\[/?b\]"
    
    return (
        pl.col(column_name)
        .str.strip_chars("[]")
        .str.replace_all("'", "")
        .str.replace_all(escape_regex, ",")
        .str.replace_all(html_regex, ",")
        .str.replace_all(";", ",")
        .str.replace_all(bbcode_regex, "")
        .str.replace_all(paren_regex, "")
        .str.replace_all("#lang_", "")
        .str.split(",")
    )


def create_silver():
    
    columns = [
        "game_id", "name", "release_date", "estimated_owners", "peak_ccu",
        "required_age", "price", "discount", "dlc_count", "about_the_game",
        "supported_languages", "full_audio_languages", "reviews", "header_image",
        "website", "support_url", "support_email", "windows", "mac", "linux",
        "metacritic_score", "metacritic_url", "user_score", "positive", "negative",
        "score_rank", "achievements", "recommendations", "notes",
        "average_playtime_forever", "average_playtime_two_weeks",
        "median_playtime_forever", "median_playtime_two_weeks",
        "developers", "publishers", "categories", "genres", "tags",
        "screenshots", "movies"
    ]
    
    lf = pl.scan_csv(BRONZE_PATH, skip_rows=1, new_columns=columns)