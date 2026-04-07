import polars as pl
from pathlib import Path
from time import time

from src.utils.etl_utils import transform_multivalued_column, clean_and_split_str, load_to_postgres

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
    
    lf = pl.scan_csv(BRONZE_PATH, skip_rows=1, has_header=False, new_columns=columns)
    
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
    
    df_languages_entity, df_game_language = many_to_many_tables["supported_languages"]
    df_full_audio = (
        lf.select(["game_id", "full_audio_languages"])
        .with_columns(clean_and_split_str("full_audio_languages").alias("name"))
        .explode("name")
        .with_columns(pl.col("name").str.strip_chars(" \r\n\t").str.replace_all(r"\s+", " "))
        .filter((pl.col("name").is_not_null()) & (pl.col("name") != ""))
        .collect()
        .join(df_languages_entity, on="name", how="inner")
        .select(["game_id", "language_id"])
        .with_columns(pl.lit(True).alias("has_full_audio"))
    )
    df_game_language = (
        df_game_language
        .join(df_full_audio, on=["game_id", "language_id"], how="left")
        .with_columns(pl.col("has_full_audio").fill_null(False))
        .unique(["game_id", "language_id"])
    )
    dataframes = [
        (table, df_game_language if table == "game_language" else df)
        for table, df in dataframes
    ]
    
    print("[INFO] Creating tables for games and metrics...")
    df_games = (
        lf.collect()
        .select([
            "game_id", "name", "release_date", "estimated_owners", "peak_ccu",
            "required_age", "price", "discount", "dlc_count", "about_the_game",
            "reviews", "header_image", "website", "support_url", "support_email",
            "windows", "mac", "linux", "notes"
        ])
        .with_columns(
            pl.coalesce([
                pl.col("release_date").str.to_date(format="%b %e, %Y", strict=False),
                pl.col("release_date").str.to_date(format="%b %Y", strict=False)
            ]).alias("release_date")
        )
    )
    
    df_metrics = (
        lf.select([
            "game_id", "metacritic_score", "metacritic_url", "user_score", "positive", "negative",
            "score_rank", "achievements", "recommendations", "average_playtime_forever",
            "average_playtime_two_weeks", "median_playtime_forever", "median_playtime_two_weeks"
        ])
        .collect()
        .with_row_index(name="metric_id", offset=1)
        .with_columns(
            pl.col("score_rank").cast(pl.Int16, strict=False).fill_null(0)
        )
    )
    
    print("[INFO] Creating tables for screenshots and movies...")
    df_screenshots, df_movies = (
        lf.with_columns(clean_and_split_str("screenshots").alias("url"))
        .explode("url")
        .filter((pl.col("url").is_not_null()) & (pl.col("url") != ""))
        .select(["game_id", "url"])
        .with_row_index(name="screenshot_id", offset=1)
        .with_columns(pl.col("screenshot_id").cast(pl.Int32))
        .collect(),
        
        lf.with_columns(clean_and_split_str("movies").alias("url"))
        .explode("url")
        .filter((pl.col("url").is_not_null()) & (pl.col("url") != ""))
        .select(["game_id", "url"])
        .with_row_index(name="movie_id", offset=1)
        .with_columns(pl.col("movie_id").cast(pl.Int32))
        .collect()
    )
    
    # 1. apenas as tabelas entidade
    for column_name, (_, entity_table, _) in many_to_many_config.items():
        dataframes.append((entity_table, many_to_many_tables[column_name][0]))

    # 2. games (precisa vir antes das associativas)
    dataframes.append(("games", df_games))

    # 3. tabelas associativas (dependem de games e das entidades)
    for column_name, (_, _, assoc_table) in many_to_many_config.items():
        dataframes.append((assoc_table, many_to_many_tables[column_name][1]))

    # 4. metrics, screenshots, movies (dependem de games)
    dataframes.append(("metrics", df_metrics))
    dataframes.append(("screenshots", df_screenshots))
    dataframes.append(("movies", df_movies))
    
    print(f"[SUCCESS] Transformed data successfully in {time() - start_time:.2f} seconds!")
    print("[INFO] Loading data to PostgreSQL...")
    load_time = time()
    try:
        load_to_postgres(dataframes)
    except Exception as e:
        raise e
    print(f"[SUCCESS] Loaded data to PostgreSQL in {time() - load_time:.2f} seconds!")
    
    print(f"[SUCCESS] Bronze -> Silver process finished in {time() - start_time:.2f} seconds!")