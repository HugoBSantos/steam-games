import polars as pl
import psycopg
from pathlib import Path
import os

from src.utils.etl_utils import transform_multivalued_column

BRONZE_PATH = Path().cwd() / "data" / "bronze" / "games.csv"


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