import polars as pl
import psycopg
import os


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


def transform_multivalued_column(lf: pl.LazyFrame, column_name: str, id_name: str):
    
    lf_exploded = (
        lf.select(["game_id", column_name])
        .with_columns(clean_and_split_str(column_name))
        .explode(column_name)
        .with_columns(
            pl.col(column_name)
            .str.strip_chars(" \r\n\t")
            .str.replace_all(r"\s+", " ")
        )
        .filter((pl.col(column_name).is_not_null()) & (pl.col(column_name) != ""))
    )
    
    df_entity = (
        lf_exploded.select(column_name)
        .unique()
        .sort(column_name)
        .collect()
        .with_row_index(name=id_name, offset=1)
    )
    
    df_associative = (
        lf_exploded.collect()
        .join(df_entity, on=column_name, how="inner")
        .select(["game_id", id_name])
    )
    
    return df_entity, df_associative