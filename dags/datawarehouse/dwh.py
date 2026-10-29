from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import COLUMNS, upsert_rows, delete_missing_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table(dag_run=None, data_interval_end=None):
    schema = 'staging'
    conn, cur = None, None

    # Дату передаёт produce_json через conf. Если update_db запущен вручную
    # без conf, берём дату его собственного запуска.
    data_date = (dag_run.conf or {}).get("data_date") or data_interval_end.to_date_string()

    try:
        YT_data = load_data(data_date)

        # Пустая выгрузка — это сбой источника, а не канал без видео.
        # Без этой проверки delete_missing_rows удалил бы из таблицы все строки.
        if not YT_data:
            raise ValueError(f"Empty extract for {data_date}: refusing to load and wipe {schema}.{table}")

        create_schema(schema)
        create_table(schema)

        # Ключи JSON из API в порядке колонок staging
        json_keys = ["video_id", "title", "published_at", "duration",
                     "view_count", "like_count", "comment_count"]
        rows = [tuple(item[k] for k in json_keys) for item in YT_data]

        # Вставка, обновление и удаление идут в одной транзакции:
        # таблица либо целиком обновится, либо останется как была.
        conn, cur = get_conn_cursor()
        upsert_rows(cur, schema, rows)
        delete_missing_rows(cur, schema, [item["video_id"] for item in YT_data])
        conn.commit()

        logger.info(f"{schema} table update completed: {len(rows)} videos from {data_date}")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error in {schema} table update: {e}")
        raise

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


@task
def core_table():
    schema = 'core'
    conn, cur = None, None

    try:
        create_schema(schema)
        create_table(schema)

        conn, cur = get_conn_cursor()
        cur.execute(f"SELECT * FROM staging.{table};")
        transformed = [transform_data(row) for row in cur.fetchall()]
        rows = [tuple(r[c] for c in COLUMNS[schema]) for r in transformed]

        # Одна транзакция, как и в staging
        upsert_rows(cur, schema, rows)
        delete_missing_rows(cur, schema, [r["Video_ID"] for r in transformed])
        conn.commit()

        logger.info(f"{schema} table update completed: {len(rows)} videos")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error in {schema} table update: {e}")
        raise

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
