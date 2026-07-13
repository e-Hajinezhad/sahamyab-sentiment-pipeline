# dags/sahamyab_sentiment_dag.py
"""
Airflow DAG for processing tweets with NEXARA sentiment model
Model is loaded once when the DAG file is parsed
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
import logging
import os
import time
# NOTE: `transformers` and `torch` are intentionally NOT imported here.
# The Airflow scheduler re-parses (imports) every DAG file constantly
# (every ~30s by default) just to detect changes and build the DAG
# graph -- it never needs the model. Importing torch/transformers at
# module level meant EVERY scheduler parse cycle paid the cost of
# importing those heavy libraries, which can take 10-30+ seconds and
# was blowing past the DagFileProcessorManager heartbeat timeout,
# causing it to be killed and restarted in a loop (DAGs never
# finished parsing, "airflow dags list" hung indefinitely).
# They're imported lazily inside get_model() instead, so the cost is
# only paid once, inside the actual task process, on first real use.

# ============================================
# Configuration
# ============================================

# ClickHouse connection (using Docker service names)
CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'admin')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DB = os.environ.get('CLICKHOUSE_DB', 'analytics')

if not CLICKHOUSE_PASSWORD:
    raise ValueError(
        "CLICKHOUSE_PASSWORD environment variable is not set. "
        "Check your .env file and docker-compose.yml."
    )

BATCH_SIZE = 50

# Model path - use environment variable or fallback
MODEL_PATH = os.environ.get('MODEL_PATH', '/opt/airflow/models/nexara')
LABELS = {
    0: "very_negative",
    1: "negative",
    2: "neutral",
    3: "positive",
    4: "very_positive"
}

# Set up logging
logger = logging.getLogger(__name__)

# ============================================
# Lazy Model Loading (loaded ONCE per worker
# process, on first actual use, not at DAG
# parse time)
# ============================================
#
# NOTE: The old version loaded the model at module level, which meant:
#   - The Airflow SCHEDULER loaded a full transformer model into memory
#     every time it parsed this DAG file (every ~30s by default) even
#     though the scheduler never needs the model at all.
#   - EVERY task in this DAG (check_unprocessed_tweets, process_tweets,
#     show_sentiment_stats) triggered a fresh model load in its own
#     process, even the two tasks that never call analyze_sentiment().
#   - This is very likely what pushed worker memory usage high enough
#     to cause OOM kills during process_tweets.
#
# Fix: use a lazy singleton. The model is only loaded the first time
# get_model() is actually called, and only within the process that
# calls it (i.e. only inside process_tweets, since that's the only
# task that calls analyze_sentiment).

_tokenizer = None
_model = None


def get_model():
    """
    Load the NEXARA tokenizer/model on first use and cache it for the
    lifetime of this worker process. Subsequent calls reuse the cached
    objects instead of reloading from disk/HuggingFace.

    torch/transformers are imported HERE (not at module top level) so
    that DAG-file parsing by the scheduler never pays their import cost.
    """
    global _tokenizer, _model

    if _model is not None:
        return _tokenizer, _model

    t_import = time.monotonic()
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    logger.info(f"⏱️  transformers import took {time.monotonic() - t_import:.2f}s")

    logger.info("=" * 70)
    logger.info("Loading NEXARA model from local path...")
    logger.info(f"📁 Model path: {MODEL_PATH}")

    if os.path.exists(MODEL_PATH):
        logger.info("✅ Model found locally! Loading from disk...")
        _tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
        _model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)
    else:
        logger.warning("⚠️  Model not found locally. Downloading from Hugging Face...")
        MODEL_NAME = "MTE313/NEXARA_model"
        _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        _model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
        logger.info("✅ Model downloaded from Hugging Face")

    _model.eval()
    logger.info("✅ Model loaded successfully!")
    logger.info(f"📊 Model classes: {LABELS}")

    return _tokenizer, _model


# ============================================
# Helper Functions
# ============================================

def analyze_sentiment(text, verbose=False):
    """
    Analyze sentiment using NEXARA model.
    Triggers the lazy model load on first call.
    """
    try:
        import torch  # lazy import, see note on get_model() above

        t_start = time.monotonic()
        tokenizer, model = get_model()

        t0 = time.monotonic()
        inputs = tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=512,
            padding=True
        )
        tokenize_time = time.monotonic() - t0

        t0 = time.monotonic()
        with torch.no_grad():
            outputs = model(**inputs)
            probs = torch.softmax(outputs.logits, dim=-1)
        inference_time = time.monotonic() - t0

        pred = torch.argmax(probs).item()

        if verbose:
            logger.info(
                f"⏱️  tokenize={tokenize_time:.3f}s inference={inference_time:.3f}s "
                f"total={time.monotonic() - t_start:.3f}s"
            )

        return {
            "sentiment_label": LABELS[pred],
            "sentiment_score": pred - 2  # -2 to +2 scale
        }

    except Exception as e:
        logger.error(f"Error analyzing text: {e}")
        return None

# ============================================
# Airflow Task Functions
# ============================================

def get_ch_client(tag=""):
    """
    Create a ClickHouse client with timing so we can tell a slow/hanging
    CONNECTION apart from a slow/hanging QUERY in the logs.
    """
    t0 = time.monotonic()
    logger.info(f"🔌 [{tag}] Opening ClickHouse connection...")
    client = Client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        connect_timeout=10,   # fail fast instead of hanging forever on a dead socket
        send_receive_timeout=60,
    )
    logger.info(f"🔌 [{tag}] Connection opened in {time.monotonic() - t0:.2f}s")
    return client


def get_unprocessed_tweets(limit=50):
    """Fetch tweets without sentiment analysis."""
    try:
        ch_client = get_ch_client("get_unprocessed_tweets")

        query = f"""
            SELECT id, content
            FROM tweets
            WHERE sentiment_label = ''
              AND content != ''
            LIMIT {limit}
        """
        t0 = time.monotonic()
        result = ch_client.execute(query)
        logger.info(f"⏱️  [get_unprocessed_tweets] query took {time.monotonic() - t0:.2f}s, rows={len(result)}")
        return result

    except Exception as e:
        logger.error(f"Error fetching tweets: {e}")
        return []

def get_total_unprocessed():
    """Get total count of unprocessed tweets."""
    try:
        ch_client = get_ch_client("get_total_unprocessed")

        query = """
            SELECT count(*)
            FROM tweets
            WHERE sentiment_label = ''
              AND content != ''
        """
        t0 = time.monotonic()
        result = ch_client.execute(query)
        logger.info(f"⏱️  [get_total_unprocessed] query took {time.monotonic() - t0:.2f}s")
        return result[0][0] if result else 0

    except Exception as e:
        logger.error(f"Error getting count: {e}")
        return 0

def update_tweet_sentiment(tweet_id, sentiment_result):
    """Update ClickHouse with sentiment results."""
    try:
        ch_client = get_ch_client(f"update_tweet_sentiment:{tweet_id}")

        label = sentiment_result['sentiment_label']
        score = sentiment_result['sentiment_score']

        query = """
            ALTER TABLE tweets
            UPDATE sentiment_label = %(label)s, sentiment_score = %(score)s
            WHERE id = %(id)s
        """
        t0 = time.monotonic()
        ch_client.execute(query, {'label': label, 'score': score, 'id': tweet_id})
        logger.info(f"⏱️  [update_tweet_sentiment] tweet={tweet_id} mutation call took {time.monotonic() - t0:.2f}s")
        return True

    except Exception as e:
        logger.error(f"Error updating tweet {tweet_id}: {e}")
        return False

def process_tweets_task(**context):
    """Main processing task for Airflow."""
    task_start = time.monotonic()
    logger.info("=" * 70)
    logger.info("Processing tweets with NEXARA sentiment model")
    logger.info("=" * 70)

    total = get_total_unprocessed()
    if total == 0:
        logger.info("✅ All tweets have been processed!")
        context['task_instance'].xcom_push(key='processed_count', value=0)
        return

    logger.info(f"📊 Total tweets to process: {total}")

    processed = 0
    failed = 0
    total_processed = 0
    batch_num = 0

    while True:
        batch_num += 1
        t_fetch = time.monotonic()
        tweets = get_unprocessed_tweets(BATCH_SIZE)
        logger.info(f"⏱️  [batch {batch_num}] fetch took {time.monotonic() - t_fetch:.2f}s")

        if not tweets:
            break

        logger.info(f"📦 [batch {batch_num}] Processing {len(tweets)} tweets...")

        for i, (tweet_id, content) in enumerate(tweets, start=1):
            t_tweet = time.monotonic()
            total_processed += 1

            # Heartbeat BEFORE work starts, so if this exact tweet hangs,
            # the log shows precisely which tweet_id/content stalled.
            logger.info(
                f"➡️  [batch {batch_num} | {i}/{len(tweets)} | overall {total_processed}/{total}] "
                f"starting tweet_id={tweet_id} len(content)={len(content) if content else 0}"
            )

            # Analyze sentiment (verbose=True logs tokenize/inference split)
            result = analyze_sentiment(content, verbose=True)

            if result:
                if update_tweet_sentiment(tweet_id, result):
                    processed += 1
                else:
                    failed += 1
            else:
                failed += 1

            logger.info(
                f"✔️  [batch {batch_num} | {i}/{len(tweets)}] "
                f"tweet_id={tweet_id} done in {time.monotonic() - t_tweet:.2f}s "
                f"(elapsed task time: {time.monotonic() - task_start:.1f}s)"
            )

        remaining = total - total_processed
        logger.info(f"📊 Progress: {total_processed}/{total} tweets")
        logger.info(f"   ✅ Success: {processed}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   📦 Remaining: {remaining}")
        logger.info(f"   ⏱️  Total elapsed: {time.monotonic() - task_start:.1f}s")

        if remaining <= 0:
            break

    logger.info("=" * 70)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 70)
    logger.info(f"✅ Successfully processed: {processed}")
    logger.info(f"❌ Failed: {failed}")
    logger.info(f"⏱️  Total task time: {time.monotonic() - task_start:.1f}s")

    context['task_instance'].xcom_push(key='processed_count', value=processed)
    context['task_instance'].xcom_push(key='failed_count', value=failed)

def check_unprocessed_tweets(**context):
    """Check if there are unprocessed tweets."""
    total = get_total_unprocessed()
    logger.info(f"📊 Unprocessed tweets: {total}")
    
    context['task_instance'].xcom_push(key='unprocessed_count', value=total)
    
    return total > 0


def get_sentiment_stats(**context):
    """Get sentiment distribution statistics."""
    try:
        ch_client = get_ch_client("get_sentiment_stats")

        query = """
            SELECT
                sentiment_label,
                count(*) AS count,
                avg(sentiment_score) AS avg_score
            FROM tweets
            WHERE sentiment_label != ''
            GROUP BY sentiment_label
            ORDER BY count DESC
        """
        stats = ch_client.execute(query)

        logger.info("=" * 70)
        logger.info("SENTIMENT DISTRIBUTION")
        logger.info("=" * 70)
        logger.info(f"{'Label':<15} {'Count':<10} {'Avg Score':<12}")
        logger.info("-" * 40)

        for label, count, avg_score in stats:
            logger.info(f"{label:<15} {count:<10} {avg_score:>+11.2f}")

        return True

    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return False
    
# ============================================
# DAG Definition
# ============================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=45),
    # If the task (or its container/host) freezes -- e.g. a laptop/Docker
    # Desktop suspend, as happened on 2026-07-05/06 -- this makes the
    # scheduler give up and mark it failed/up_for_retry after 45 minutes
    # instead of waiting indefinitely for a possibly-frozen process to
    # wake back up and get caught by the heartbeat check.
}

with DAG(
    'sahamyab_sentiment_analysis',
    default_args=default_args,
    description='Process tweets with NEXARA sentiment model',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['sahamyab', 'sentiment', 'ml'],
    doc_md="""
    ### Sahamyab Sentiment Analysis DAG
    
    This DAG processes unlabeled tweets using the NEXARA sentiment model.
    The model is loaded once at startup from the local `models/` folder.
    
    **NEXARA Labels:**
    - very_negative (score: -2)
    - negative (score: -1)
    - neutral (score: 0)
    - positive (score: +1)
    - very_positive (score: +2)
    """
) as dag:

    start_task = DummyOperator(
        task_id='start',
        doc_md="Start of the sentiment analysis pipeline"
    )

    check_tweets_task = PythonOperator(
        task_id='check_unprocessed_tweets',
        python_callable=check_unprocessed_tweets,
        doc_md="Check if there are unprocessed tweets"
    )

    process_tweets_task = PythonOperator(
        task_id='process_tweets',
        python_callable=process_tweets_task,
        doc_md="Process tweets with NEXARA sentiment model"
    )

    show_stats_task = PythonOperator(
        task_id='show_sentiment_stats',
        python_callable=get_sentiment_stats,
        doc_md="Display sentiment distribution statistics"
    )

    skip_task = DummyOperator(
        task_id='skip_processing',
        doc_md="Skip processing if no unprocessed tweets"
    )

    end_task = DummyOperator(
        task_id='end',
        doc_md="End of the sentiment analysis pipeline"
    )

    start_task >> check_tweets_task
    check_tweets_task >> process_tweets_task >> show_stats_task >> end_task
    check_tweets_task >> skip_task >> end_task