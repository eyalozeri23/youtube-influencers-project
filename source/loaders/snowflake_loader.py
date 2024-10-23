import pandas as pd
from snowflake.connector import connect
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def load_to_snowflake(input_path):
    
    try:
        df = pd.read_csv(input_path)
        
        conn = connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WH'),
            database=os.getenv('SNOWFLAKE_DB'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS INFLUENCER_CAMPAIGN_METRICS (
            Influencer_Name STRING,
            Video_Url STRING,
            Campaign_name STRING,
            publish_date DATE,
            current_likes_count INTEGER,
            interval_date DATE
        )
        """)

    
        # Update interval_date
        df['interval_date'] = datetime.now().date()
        

        # Create staging table for new data
        cursor.execute("""
        CREATE OR REPLACE TEMPORARY TABLE TEMP_METRICS (
            Influencer_Name STRING,
            Video_Url STRING,
            Campaign_name STRING,
            publish_date DATE,
            current_likes_count INTEGER,
            interval_date DATE
        )
        """)

        # Insert data into staging table
        insert_sql = """
        INSERT INTO TEMP_METRICS (
            Influencer_Name, Video_Url, Campaign_name,
            publish_date, current_likes_count, interval_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        data_tuples = [
            (row['Influencer_Name'], row['Video_Url'], row['Campaign_name'],
             row['publish_date'], row['current_likes_count'], row['interval_date'])
            for _, row in df.iterrows()
        ]
        
        cursor.executemany(insert_sql, data_tuples)

        # Perform merge operation
        merge_sql = """
        MERGE INTO INFLUENCER_CAMPAIGN_METRICS target
        USING TEMP_METRICS source
        ON target.Video_Url = source.Video_Url 
           AND target.interval_date = source.interval_date
        WHEN MATCHED THEN
            UPDATE SET
                current_likes_count = source.current_likes_count,
                interval_date = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                Influencer_Name, Video_Url, Campaign_name,
                publish_date, current_likes_count, interval_date
            )
            VALUES (
                source.Influencer_Name,
                source.Video_Url,
                source.Campaign_name,
                source.publish_date,
                source.current_likes_count,
                source.interval_date
            )
        """
        cursor.execute(merge_sql)

        # Log the results
        cursor.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN last_updated_at > DATEADD(minute, -5, CURRENT_TIMESTAMP()) 
                      THEN 1 END) as updated_records
        FROM INFLUENCER_CAMPAIGN_METRICS
        WHERE interval_date = CURRENT_DATE()
        """)
        
        total_records, updated_records = cursor.fetchone()
        logging.info(f"Processed {len(df)} records:")
        logging.info(f"- Total records in table: {total_records}")
        logging.info(f"- Updated/Inserted records: {updated_records}")

        
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"Error loading data to Snowflake: {str(e)}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
