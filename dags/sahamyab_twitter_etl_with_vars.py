# dags/sahamyab_twitter_etl.py
"""
Airflow DAG for Sahamyab Twitter ETL Pipeline
Extracts tweets from Sahamyab API, stores raw data in MongoDB, 
transforms and loads into ClickHouse
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import requests
import pymongo
from clickhouse_driver import Client
from dateutil import parser
import json
import logging
import os

# ============================================
# Configuration
# ============================================

# Database connection settings - read from environment variables.
# These are set in docker-compose.yml, which reads them from your .env file.
MONGO_URI = os.environ.get('MONGO_URI')
MONGO_DB = "analytics"
MONGO_COLLECTION = "raw_tweets"

CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'admin')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DB = os.environ.get('CLICKHOUSE_DB', 'analytics')

if not MONGO_URI:
    raise ValueError(
        "MONGO_URI environment variable is not set. "
        "Check your .env file and docker-compose.yml."
    )
if not CLICKHOUSE_PASSWORD:
    raise ValueError(
        "CLICKHOUSE_PASSWORD environment variable is not set. "
        "Check your .env file and docker-compose.yml."
    )

SAHAMYAB_URL = "https://www.sahamyab.com/guest/twiter/list?v=0.1"
USER_AGENT = "Chrome/133.0"

# Set up logging
logger = logging.getLogger(__name__)

# ============================================
# Task 1: Extract Tweets
# ============================================

def extract_tweets(**context):
    """
    Fetch tweets from Sahamyab API and store raw in MongoDB
    """
    logger.info("=" * 60)
    logger.info("TASK 1: EXTRACTING TWEETS FROM SAHAMYAB")
    logger.info("=" * 60)

    try:
        # 1. Call the API
        headers = {"User-Agent": USER_AGENT}
        logger.info(f"📡 Fetching data from: {SAHAMYAB_URL}")
        response = requests.get(SAHAMYAB_URL, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        logger.info(f"✅ API call successful!")
        logger.info(f"   - Status Code: {response.status_code}")
        
        if 'items' in data:
            logger.info(f"   - Number of tweets: {len(data['items'])}")
        logger.info(f"   - Success: {data.get('success', False)}")

        # 2. Store in MongoDB
        logger.info(f"\n💾 Storing raw data in MongoDB...")
        mongo_client = pymongo.MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB]
        collection = mongo_db[MONGO_COLLECTION]

        result = collection.insert_one({
            "timestamp": datetime.now(),
            "source": "sahamyab_twitter",
            "raw_data": data
        })

        logger.info(f"✅ Raw data stored in MongoDB!")
        logger.info(f"   - Document ID: {result.inserted_id}")

        # 3. Push to XCom for the next task
        context['task_instance'].xcom_push(key='raw_data', value=data)
        context['task_instance'].xcom_push(key='tweet_count', value=len(data.get('items', [])))

        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Error in extraction: {e}")
        raise

# ============================================
# Task 2: Transform & Load Tweets
# ============================================

def transform_and_load_tweets(**context):
    """
    Transform tweets and load into ClickHouse
    """
    logger.info("\n" + "=" * 60)
    logger.info("TASK 2: TRANSFORMING & LOADING TO CLICKHOUSE")
    logger.info("=" * 60)

    try:
        # 1. Get raw data from XCom
        raw_data = context['task_instance'].xcom_pull(key='raw_data')
        
        if not raw_data:
            logger.info("📂 No data in XCom, fetching from MongoDB...")
            mongo_client = pymongo.MongoClient(MONGO_URI)
            mongo_db = mongo_client[MONGO_DB]
            collection = mongo_db[MONGO_COLLECTION]
            
            latest_doc = collection.find_one(
                {"source": "sahamyab_twitter"},
                sort=[("timestamp", pymongo.DESCENDING)]
            )
            
            if not latest_doc:
                logger.error("❌ No data found in MongoDB!")
                return
            
            raw_data = latest_doc.get("raw_data", {})
            logger.info(f"✅ Found data from: {latest_doc['timestamp']}")

        # 2. Extract tweets from the raw data
        if 'items' not in raw_data or not raw_data['items']:
            logger.warning("❌ No 'items' found in the data or items is empty!")
            return

        tweets_data = raw_data['items']
        logger.info(f"📊 Found {len(tweets_data)} tweets to process")

        # 3. Transform each tweet
        logger.info(f"\n🔄 Transforming tweets...")
        clean_tweets = []
        
        for tweet in tweets_data:
            # Convert sendTime string to datetime
            send_time_str = tweet.get('sendTime')
            if send_time_str:
                try:
                    send_time = parser.parse(send_time_str)
                except:
                    send_time = datetime.now()
            else:
                send_time = datetime.now()
            
            clean_tweet = {
                'id': str(tweet.get('id', '')),
                'send_time': send_time,
                'send_time_persian': tweet.get('sendTimePersian', ''),
                'sender_name': tweet.get('senderName', ''),
                'sender_username': tweet.get('senderUsername', ''),
                'content': tweet.get('content', ''),
                'type': tweet.get('type', ''),
                'comment_count': int(tweet.get('commentCount', 0)),
                'has_parent': 1 if tweet.get('parentId') else 0,
                'parent_id': str(tweet.get('parentId', '')),
                'parent_content': tweet.get('parentContent', ''),
                'parent_sender_name': tweet.get('parentSenderName', ''),
            }
            clean_tweets.append(clean_tweet)

        logger.info(f"✅ Transformed {len(clean_tweets)} tweets")

        # 4. Load into ClickHouse
        logger.info(f"\n💾 Loading to ClickHouse...")
        logger.info(f"   - Connecting to: {CLICKHOUSE_HOST}:9000")
        
        ch_client = Client(
            host=CLICKHOUSE_HOST,
            port=9000,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB
        )

        # Test connection
        try:
            version = ch_client.execute("SELECT version()")
            logger.info(f"   ✅ Connected to ClickHouse version: {version[0][0]}")
        except Exception as e:
            logger.error(f"   ❌ Failed to connect to ClickHouse: {e}")
            raise

        # Get count before insert
        before_count = ch_client.execute("SELECT count(*) FROM tweets")[0][0]
        logger.info(f"   - Rows before insert: {before_count}")

        # Insert data
        if clean_tweets:
            ch_client.execute(
                '''INSERT INTO analytics.tweets 
                   (id, send_time, send_time_persian, sender_name, sender_username, 
                    content, type, comment_count, has_parent, parent_id, parent_content, parent_sender_name) 
                   VALUES''',
                clean_tweets
            )
            logger.info(f"✅ Inserted {len(clean_tweets)} tweets into ClickHouse!")

            after_count = ch_client.execute("SELECT count(*) FROM tweets")[0][0]
            logger.info(f"   - Rows after insert: {after_count}")
            logger.info(f"   - New rows added: {after_count - before_count}")

            # Push stats to XCom
            context['task_instance'].xcom_push(key='rows_inserted', value=after_count - before_count)

            # Show stats by type
            logger.info(f"\n📊 Quick stats from ClickHouse:")
            stats = ch_client.execute(
                """
                SELECT 
                    type,
                    count(*) AS count,
                    min(send_time) AS earliest,
                    max(send_time) AS latest
                FROM tweets 
                GROUP BY type 
                ORDER BY count DESC
                """
            )
            
            logger.info(f"   {'Type':<15} {'Count':<10} {'Earliest':<20} {'Latest':<20}")
            logger.info(f"   {'-'*65}")
            for row in stats:
                logger.info(f"   {row[0]:<15} {row[1]:<10} {str(row[2]):<20} {str(row[3]):<20}")

    except Exception as e:
        logger.error(f"❌ Error in transformation: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

# ============================================
# Task 3: Verify Pipeline
# ============================================

def verify_pipeline(**context):
    """
    Verify that the pipeline is working correctly
    """
    logger.info("\n" + "=" * 60)
    logger.info("TASK 3: VERIFYING PIPELINE")
    logger.info("=" * 60)

    try:
        # Check MongoDB
        logger.info("\n📂 Checking MongoDB...")
        mongo_client = pymongo.MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB]
        collection = mongo_db[MONGO_COLLECTION]
        
        count = collection.count_documents({"source": "sahamyab_twitter"})
        logger.info(f"   - Raw tweets documents in MongoDB: {count}")
        
        if count > 0:
            latest = collection.find_one(
                {"source": "sahamyab_twitter"},
                sort=[("timestamp", pymongo.DESCENDING)]
            )
            logger.info(f"   - Latest document timestamp: {latest['timestamp']}")

        # Check ClickHouse
        logger.info("\n🗄️ Checking ClickHouse...")
        ch_client = Client(
            host=CLICKHOUSE_HOST,
            port=9000,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB
        )

        total_count = ch_client.execute("SELECT count(*) FROM tweets")[0][0]
        logger.info(f"   - Total tweets in ClickHouse: {total_count}")

        if total_count > 0:
            # Show distribution by type
            type_counts = ch_client.execute(
                "SELECT type, count(*) as cnt FROM tweets GROUP BY type ORDER BY cnt DESC"
            )
            logger.info(f"\n   📊 Tweets by type:")
            for type_name, cnt in type_counts:
                logger.info(f"      - {type_name}: {cnt}")

            # Show latest tweets
            latest = ch_client.execute(
                "SELECT sender_name, content FROM tweets ORDER BY send_time DESC LIMIT 3"
            )
            logger.info(f"\n   📝 3 most recent tweets:")
            for sender, content in latest:
                logger.info(f"      - @{sender}: {content[:80]}...")

        logger.info("\n✅ Pipeline verification complete!")

        # Push success status
        context['task_instance'].xcom_push(key='verification_status', value='success')

    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

# ============================================
# DAG Definition
# ============================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'catchup': False,
}

with DAG(
    'sahamyab_twitter_etl',
    default_args=default_args,
    description='Extract tweets from Sahamyab, store raw in MongoDB, clean in ClickHouse',
    schedule_interval='*/20 * * * *',  # Every 20 minutes
    max_active_runs=1,
    tags=['sahamyab', 'twitter', 'etl', 'banking'],
    doc_md="""
    ### Sahamyab Twitter ETL Pipeline
    
    This DAG extracts tweets from the Sahamyab API and loads them into ClickHouse.
    
    **Pipeline Steps:**
    1. **Extract**: Fetch tweets from Sahamyab API
    2. **Load (Raw)**: Store raw JSON in MongoDB
    3. **Transform**: Clean and structure the data
    4. **Load (Analytical)**: Insert into ClickHouse
    5. **Verify**: Check data integrity
    
    **Data Flow:**
    Sahamyab API → MongoDB (Raw) → ClickHouse (Analytical)
    """
) as dag:

    # ============================================
    # Task Definitions
    # ============================================

    start_task = DummyOperator(
        task_id='start',
        doc_md="Start of the ETL pipeline"
    )

    extract_task = PythonOperator(
        task_id='extract_tweets',
        python_callable=extract_tweets,
        doc_md="Fetch tweets from Sahamyab API and store raw in MongoDB"
    )

    transform_task = PythonOperator(
        task_id='transform_and_load_tweets',
        python_callable=transform_and_load_tweets,
        doc_md="Transform tweets and load into ClickHouse"
    )

    verify_task = PythonOperator(
        task_id='verify_pipeline',
        python_callable=verify_pipeline,
        doc_md="Verify that the pipeline completed successfully"
    )

    end_task = DummyOperator(
        task_id='end',
        doc_md="End of the ETL pipeline"
    )

    # ============================================
    # Task Dependencies
    # ============================================

    start_task >> extract_task >> transform_task >> verify_task >> end_task


# Schedule	Cron Expression
# Every 15 minutes	*/15 * * * *
# Every hour	    0 * * * *
# Every 30 minutes	*/30 * * * *
# Every 5 minutes	*/5 * * * *
# Daily at midnight	0 0 * * *
