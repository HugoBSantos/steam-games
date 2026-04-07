import polars as pl
import psycopg
from pathlib import Path
import os
import io

def load_to_postgres(dataframes: list[tuple[str, pl.DataFrame]]):
    
    postgres_url = os.getenv("POSTGRES_URL")
    ddl_path = Path().cwd() / "sql" / "ddl" / "silver.sql"
    
    with psycopg.connect(postgres_url) as conn:
        with conn.cursor() as cur:
            with open(ddl_path, mode="r") as f:
                cur.execute(f.read())

            for table_name, df in dataframes:
                buffer = io.BytesIO()
                
                df.write_csv(buffer, include_header=False)
                buffer.seek(0)
                
                columns_str = ", ".join(df.columns)
                copy_sql = f"COPY silver.{table_name} ({columns_str}) FROM STDIN WITH (FORMAT csv)"
                
                with cur.copy(copy_sql) as copy:
                    copy.write(buffer.read())
        
        conn.commit()

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


def transform_multivalued_column(lf: pl.LazyFrame, column_name: str, id_name: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    
    lf_exploded = (
        lf.select(["game_id", column_name])
        .with_columns(clean_and_split_str(column_name).alias("name"))
        .explode("name")
        .with_columns(
            pl.col("name")
            .str.strip_chars(" \r\n\t")
            .str.replace_all(r"\s+", " ")
        )
        .filter((pl.col("name").is_not_null()) & (pl.col("name") != ""))
    )
    
    df_entity = (
        lf_exploded.select("name")
        .unique()
        .sort("name")
        .collect()
        .with_row_index(name=id_name, offset=1)
        .with_columns(pl.col(id_name).cast(pl.Int32))
    )
    
    df_associative = (
        lf_exploded.collect()
        .join(df_entity, on="name", how="inner")
        .select(["game_id", id_name])
        .unique(["game_id", id_name])
        .with_columns(
            pl.col("game_id").cast(pl.Int32),
            pl.col(id_name).cast(pl.Int32)
        )
    )
    
    return df_entity, df_associative