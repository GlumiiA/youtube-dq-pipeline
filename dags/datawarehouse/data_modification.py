import logging

from psycopg2 import sql
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)
table = "yt_api"

# Колонки таблицы в каждой схеме. Ключ "Video_ID" — первичный ключ.
COLUMNS = {
    "staging": ["Video_ID", "Video_Title", "Upload_Date", "Duration",
                "Video_Views", "Likes_Count", "Comments_Count"],
    "core": ["Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type",
             "Video_Views", "Likes_Count", "Comments_Count"],
}
KEY = "Video_ID"


def upsert_rows(cur, schema, rows):
    """Вставляет новые видео и обновляет существующие одной командой.

    rows — список кортежей в порядке COLUMNS[schema].
    INSERT ... ON CONFLICT по первичному ключу делает загрузку идемпотентной:
    повторный запуск с теми же данными даёт тот же результат, без дублей.
    """
    columns = COLUMNS[schema]
    updates = [c for c in columns if c != KEY]

    query = sql.SQL("""
        INSERT INTO {schema}.{table} ({columns})
        VALUES %s
        ON CONFLICT ({key}) DO UPDATE SET {updates};
    """).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        key=sql.Identifier(KEY),
        updates=sql.SQL(", ").join(
            sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c)) for c in updates
        ),
    )

    execute_values(cur, query, rows, page_size=500)
    logger.info(f"Upserted {len(rows)} rows into {schema}.{table}")


def delete_missing_rows(cur, schema, keep_ids):
    """Удаляет видео, которых больше нет в свежей выгрузке (удалены или скрыты на канале)."""
    query = sql.SQL('DELETE FROM {schema}.{table} WHERE {key} <> ALL(%s);').format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        key=sql.Identifier(KEY),
    )
    cur.execute(query, (list(keep_ids),))
    logger.info(f"Deleted {cur.rowcount} rows from {schema}.{table} that are no longer on the channel")
