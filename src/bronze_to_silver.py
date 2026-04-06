import polars as pl
from pathlib import Path
from time import time

from src.utils.etl_utils import transform_multivalued_column, clean_and_split_str

BRONZE_PATH = Path().cwd() / "data" / "bronze" / "games.csv"


def create_silver():
    
    start_time = time()
    
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
    
    print("[INFO] Creating dataframes for many-to-many tables...")
    many_to_many_config = {
        "supported_languages": ("language_id", "supported_languages", "game_language"),
        "categories":          ("category_id", "categories",          "game_category"),
        "genres":              ("genre_id",    "genres",              "game_genre"),
        "tags":                ("tag_id",      "tags",                "tags_games"),
        "developers":          ("developer_id","developers",          "game_developer"),
        "publishers":          ("publisher_id","publishers",          "game_publisher"),
    }
    many_to_many_tables = {
        table_name: transform_multivalued_column(lf, table_name, id_name)
        for table_name, (id_name, _, _) in many_to_many_config.items()
    }
    for column_name, (_, entity_table, assoc_table) in many_to_many_config.items():
        dfs = many_to_many_tables[column_name]
        dataframes.append((entity_table, dfs[0]))
        dataframes.append((assoc_table,  dfs[1]))
    
    print("[INFO] Creating tables for games and metrics...")
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
    
    print("[INFO] Creating tables for screenshots and movies...")
    df_screenshots, df_movies = (
        lf.with_columns(clean_and_split_str("screenshots").alias("url"))
        .explode("url")
        .filter((pl.col("url").is_not_null()) & (pl.col("url") != ""))
        .select(["game_id", "url"])
        .with_row_index(name="screenshot_id", offset=1)
        .collect(),
        
        lf.with_columns(clean_and_split_str("movies").alias("url"))
        .explode("url")
        .filter((pl.col("url").is_not_null()) & (pl.col("url") != ""))
        .select(["game_id", "url"])
        .with_row_index(name="movie_id", offset=1)
        .collect()
    )
    dataframes.append(("screenshots", df_screenshots))
    dataframes.append(("movies", df_movies))
    
    print(f"[INFO] Transformed data successfully in {time() - start_time:.2f} seconds!")