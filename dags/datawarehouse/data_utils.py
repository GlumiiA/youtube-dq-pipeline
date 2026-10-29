from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql, errors
from psycopg2.extras import RealDictCursor # Import RealDictCursor for fetching results as dictionaries

table = 'yt_api'

def get_conn_cursor(): # Function to establish a connection and cursor to the PostgreSQL database. 
    hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt', database='elt_db') # Create a PostgresHook using the connection ID and database name
    conn = hook.get_conn() # Get a connection object from the hook
    cur = conn.cursor(cursor_factory=RealDictCursor) # Create a cursor object for executing SQL and fetching rows as dictionaries
    return conn, cur # Return the connection and cursor objects
    
def close_conn_cursor(conn, cur): # Function to close the cursor and connection to the PostgreSQL database.
    cur.close() # Close the cursor to free up resources
    conn.close() # Close the connection to free up resources

def create_schema(schema):
    con, cur = get_conn_cursor() # Establish a connection and cursor to the database
    schema_sql = sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};").format(
        schema=sql.Identifier(schema)
    )
    try:
        cur.execute(schema_sql) # Execute the SQL statement
        con.commit() # Commit the transaction
    except errors.UniqueViolation:
        con.rollback()
    finally:
        close_conn_cursor(con, cur) # Close the connection and cursor

def create_table(schema):
    con, cur = get_conn_cursor() # Establish a connection and cursor to the database
    if schema == 'staging':
        table_sql = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT, 
                "Likes_Count" INT,
                "Comments_Count" INT
            );
        """).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table)
        ) # SQL statement to create a table with specified columns and data types
    else:
        table_sql = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" TIME NOT NULL,
                "Video_Type" VARCHAR(10) NOT NULL,
                "Video_Views" INT, 
                "Likes_Count" INT,
                "Comments_Count" INT
            );
        """).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table)
        ) # SQL statement to create a table with specified columns and data types (same as staging in this case)

    cur.execute(table_sql) # Execute the SQL statement
    con.commit() # Commit the transaction
    close_conn_cursor(con, cur) # Close the connection and cursor

def get_video_ids(cur, schema):
    cur.execute(
        sql.SQL('SELECT "Video_ID" FROM {schema}.{table};').format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table)
        )
    ) # Execute a SQL query to select all video IDs from the specified schema and table
    ids = cur.fetchall() # Fetch all results from the executed query
    video_ids = [row["Video_ID"] for row in ids] # Extract video IDs from the fetched results and create a list of video IDs
    return video_ids # Return the list of video IDs