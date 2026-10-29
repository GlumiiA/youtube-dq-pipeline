import logging
from psycopg2 import sql

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row):

    try:
        if schema == 'staging':
            video_id = 'video_id'
            cur.execute(
                sql.SQL("""INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(published_at)s, %(duration)s, %(view_count)s, %(like_count)s, %(comment_count)s);""").format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table)
                ),
                row
            )
        else:
            video_id = 'Video_ID'
            cur.execute(
                sql.SQL("""INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s);""").format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table)
                ),
                row
            )

        conn.commit() # Commit the transaction to save changes to the database
        logger.info(f"Inserted row with Video ID: {row[video_id]}") # Log the successful insertion of the row

    except Exception as e:
        logger.error(f"Error inserting row with Video ID: {row[video_id]}") # Log any errors that occur during the insertion process
        raise e # Re-raise the exception to be handled by the calling code
    
def update_rows(cur, conn, schema, row):

    try:
        #staging
        if schema == 'staging':
            video_id = 'video_id'
            upload_date = 'published_at'
            video_title = 'title'
            video_views = 'view_count'
            likes_count = 'like_count'
            comments_count = 'comment_count'
        #core
        else:
            video_id = 'Video_ID'
            upload_date = 'Upload_Date'
            video_title = 'Video_Title'
            video_views = 'Video_Views'
            likes_count = 'Likes_Count'
            comments_count = 'Comments_Count'

        # Build the query string by injecting the placeholder *names* (like 'title' or 'Video_Title')
        # into the %(name)s placeholders using an f-string, then format the schema/table
        # with sql.Identifier to avoid Python's .format() trying to replace those placeholder names.
        query_str = f"""
            UPDATE {{schema}}.{{table}}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """

        query = sql.SQL(query_str).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table)
        )

        cur.execute(query, row)

        conn.commit() # Commit the transaction to save changes to the database
        logger.info(f"Updated row with Video ID: {row[video_id]}") # Log the successful update of the row

    except Exception as e:
        logger.error(f"Error updating row with Video ID: {row[video_id]} - {str(e)}") # Log any errors that occur during the update process
        raise e # Re-raise the exception to be handled by the calling code
    
def delete_rows(cur, conn, schema, ids_to_delete):

    try:
        delete_list = tuple(ids_to_delete)
        query = sql.SQL(
            """
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN %s;
            """
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table)
        )

        cur.execute(query, (delete_list,))

        conn.commit() # Commit the transaction to save changes to the database
        logger.info(f"Deleted rows with Video ID: {delete_list}") # Log the successful deletion of the rows

    except Exception as e:
        logger.error(f"Error deleting rows with Video ID: {ids_to_delete} - {str(e)}") # Log any errors that occur during the deletion process
        raise e # Re-raise the exception to be handled by the calling code