from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from datetime import datetime, timedelta

def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_session_summary_table():
    cursor = get_snowflake_cursor()
    try:
        # Create the analytics schema if it doesn't exist and the session_summary table
        cursor.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analytics.session_summary (
                userId INT NOT NULL,
                sessionId VARCHAR(32) PRIMARY KEY,
                channel VARCHAR(32),
                ts TIMESTAMP
            );
        """)
        print("Schema `analytics` and table `session_summary` created successfully")
    except Exception as e:
        print(f"Error creating session_summary table: {e}")
        raise

@task
def populate_session_summary():
    cursor = get_snowflake_cursor()
    try:
        # Insert joined data, checking for duplicates by sessionId
        cursor.execute("""
            INSERT INTO analytics.session_summary (userId, sessionId, channel, ts)
            SELECT DISTINCT 
                usc.userId, 
                usc.sessionId, 
                usc.channel, 
                st.ts
            FROM dev.raw_data.user_session_channel usc
            JOIN dev.raw_data.session_timestamp st
            ON usc.sessionId = st.sessionId
            WHERE usc.sessionId NOT IN (SELECT sessionId FROM analytics.session_summary);
        """)
        print("Data inserted into `session_summary` successfully")
    except Exception as e:
        print(f"Error inserting data into session_summary: {e}")
        raise

with DAG(
    dag_id='ELT_SessionSummary',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    schedule_interval='@daily',
    tags=['ELT']
) as dag:
    
    # Define task order: create table with schema, then populate
    create_table_task = create_session_summary_table()
    populate_data_task = populate_session_summary()

    create_table_task >> populate_data_task
