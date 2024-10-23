import pandas as pd
from snowflake.connector import connect
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def load_to_snowflake(df):
    conn = None
    cursor = None
    
    try:
        # Validate input DataFrame
        required_columns = ['Influencer_Name', 'Video_Url', 'Campaign_name', 
                          'publish_date', 'current_likes_count']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

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
            interval_date DATE,
            last_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
    
        # Update interval_date
        df['interval_date'] = datetime.now().date()
        
        # Create staging table
        cursor.execute("""
        CREATE OR REPLACE TEMPORARY TABLE TEMP_METRICS (
            Influencer_Name STRING,
            Video_Url STRING,
            Campaign_name STRING,
            publish_date DATE,
            current_likes_count INTEGER,
            interval_date DATE,
            last_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)

        # Insert data using executemany
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
        logging.info(f"Inserted {len(data_tuples)} records into staging table")

        # Perform merge
        merge_sql = """
        MERGE INTO INFLUENCER_CAMPAIGN_METRICS target
        USING TEMP_METRICS source
        ON target.Video_Url = source.Video_Url 
           AND target.interval_date = source.interval_date
        WHEN MATCHED THEN
            UPDATE SET
                current_likes_count = source.current_likes_count,
                last_updated_at = CURRENT_TIMESTAMP()
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
        logging.info("Merge operation completed")

        # Get results
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

        conn.commit()
        logging.info("Changes committed successfully")
        
    except Exception as e:
        if conn:
            conn.rollback()
            logging.error("Transaction rolled back")
        raise Exception(f"Error loading data to Snowflake: {str(e)}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("Database connection closed")
