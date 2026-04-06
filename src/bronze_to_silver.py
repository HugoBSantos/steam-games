import polars as pl
from pathlib import Path

from src.utils.etl_utils import transform_multivalued_column

BRONZE_PATH = Path().cwd() / "data" / "bronze" / "games.csv"


def create_silver():
    
    dataframes = []
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
    
    many_to_many_tables = {
        column_name: transform_multivalued_column(lf, column_name, id_name)
        
        for column_name, id_name in {
            "supported_languages": "language_id",
            "categories": "category_id",
            "genres": "genre_id",
            "tags": "tag_id",
            "developers": "developer_id",
            "publishers": "publisher_id"
        }.items()
    }
    for table_name, dfs in many_to_many_tables.items():
        dataframes.append((table_name, dfs[0]))
        dataframes.append((table_name, dfs[1]))
    
    df_games = (
        lf.collect()
        .join(many_to_many_tables["developers"][1], on="game_id", how="left")
        .join(many_to_many_tables["publishers"][1], on="game_id", how="left")
        .select([
            "game_id", "developer_id", "publisher_id", "name", "release_date", "estimated_owners",
            "peak_ccu", "required_age", "price", "discount", "dlc_count", "about_the_game",
            "reviews", "header_image", "website", "support_url", "support_email",
            "windows", "mac", "linux", "notes"
        ])
    )
    dataframes.append(("games", df_games))
    print(df_games.head(3))
    
    df_metrics = (
        lf.select([
            "game_id", "metacritic_score", "metacritic_url", "user_score", "positive", "negative",
            "score_rank", "achievements", "recommendations", "average_playtime_forever",
            "average_playtime_two_weeks", "median_playtime_forever", "median_playtime_two_weeks"
        ])
        .collect()
        .with_row_index(name="metric_id", offset=1)
    )
    dataframes.append(("metrics", df_metrics))
    print(df_metrics.head(3))