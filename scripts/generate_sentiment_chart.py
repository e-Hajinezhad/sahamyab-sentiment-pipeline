"""
generate_sentiment_chart.py

Reads sentiment-labeled tweets from analytics.tweets (not from the
sentiment_daily_stats rollup table, which only has day-level granularity
and can lag behind until its next scheduled refresh) and produces two
charts, each limited to the last 10 days:

  1. Hourly sentiment breakdown
  2. Daily sentiment breakdown

This is a standalone script, meant to be run on your own machine (not
inside Airflow), since ClickHouse's port is already exposed to localhost
by docker-compose.yml.

Output:
  - sentiment_chart_hourly.png / docs/sentiment_chart_hourly.html
  - sentiment_chart_daily.png  / docs/sentiment_chart_daily.html

Note: PNG export uses matplotlib, not kaleido. Kaleido launches an
embedded headless Chromium over a local network port, which can trigger
a Windows Firewall prompt -- and if that prompt is blocked or ignored,
the script hangs indefinitely with no error, since it's waiting on OS
permission, not something Python can time out or catch. matplotlib
renders in-process with no subprocess/network involved, so this can't
happen.

Usage:
    pip install -r requirements-dev.txt
    python scripts/generate_sentiment_chart.py
"""

import os
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import jdatetime
from clickhouse_driver import Client
from dotenv import load_dotenv

# ---------------------------------------------------------------
# Config
# ---------------------------------------------------------------
DAYS_TO_SHOW = 10

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST_EXTERNAL", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT_EXTERNAL", "9000"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "analytics")

if not CLICKHOUSE_USER or not CLICKHOUSE_PASSWORD:
    print("ERROR: CLICKHOUSE_USER / CLICKHOUSE_PASSWORD not found.")
    print(f"Make sure .env exists at: {PROJECT_ROOT / '.env'}")
    sys.exit(1)

# Fixed color mapping, so "negative" is always red-ish, "positive" always
# green-ish, etc, no matter what order the data comes in or which labels
# are present.
COLOR_MAP = {
    "very_negative": "#b71c1c",
    "negative": "#e57373",
    "neutral": "#9e9e9e",
    "positive": "#81c784",
    "very_positive": "#1b5e20",
}
LABEL_ORDER = ["very_negative", "negative", "neutral", "positive", "very_positive"]

docs_dir = PROJECT_ROOT / "docs"
docs_dir.mkdir(exist_ok=True)

print(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} ...")
client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB,
)


def load_data(bucket_expr: str, bucket_col: str) -> pd.DataFrame:
    """
    Query analytics.tweets, bucketed by the given ClickHouse time
    expression (e.g. toStartOfHour(send_time) or toDate(send_time)),
    limited to the last DAYS_TO_SHOW days, and compute each bucket's
    sentiment percentage ourselves in pandas.
    """
    query = f"""
        SELECT
            {bucket_expr} AS {bucket_col},
            sentiment_label,
            count(*) AS tweet_count
        FROM analytics.tweets
        WHERE sentiment_label != ''
          AND send_time >= now() - INTERVAL {DAYS_TO_SHOW} DAY
        GROUP BY {bucket_col}, sentiment_label
        ORDER BY {bucket_col}
    """
    rows = client.execute(query)
    if not rows:
        return pd.DataFrame(columns=[bucket_col, "sentiment_label", "tweet_count", "percentage"])

    df = pd.DataFrame(rows, columns=[bucket_col, "sentiment_label", "tweet_count"])
    df["percentage"] = df.groupby(bucket_col)["tweet_count"].transform(
        lambda counts: 100.0 * counts / counts.sum()
    )
    return df


def build_and_export(df: pd.DataFrame, bucket_col: str, persian_fmt: str, title: str, filename_base: str, max_ticks: int = 15):
    """
    Given a bucketed+percentage dataframe, add a Persian date/time label
    column, then build and save both the interactive HTML (plotly) and
    the static PNG (matplotlib) versions of the chart.
    """
    if df.empty:
        print(f"No data for '{filename_base}' in the last {DAYS_TO_SHOW} days -- skipping.")
        return

    is_date_only = "date" in bucket_col and "hour" not in bucket_col

    def to_persian(value):
        if is_date_only:
            return jdatetime.date.fromgregorian(date=value).strftime(persian_fmt)
        return jdatetime.datetime.fromgregorian(datetime=value).strftime(persian_fmt)

    df = df.copy()
    df["persian_label"] = df[bucket_col].apply(to_persian)

    # Plotly Express orders categorical axes/colors by order of first
    # appearance in the dataframe by default, NOT by sorting them --
    # unlike the matplotlib chart below, where we explicitly sort/reindex.
    # Force both orderings explicitly so the two charts always agree:
    #   - x-axis: chronological (zero-padded labels sort correctly as text)
    #   - color/stacking: fixed very_negative -> very_positive order
    sorted_labels = sorted(df["persian_label"].unique())
    sentiment_order = [c for c in LABEL_ORDER if c in df["sentiment_label"].unique()]

    # --- Plotly (interactive HTML) ---
    fig = px.area(
        df,
        x="persian_label",
        y="percentage",
        color="sentiment_label",
        color_discrete_map=COLOR_MAP,
        category_orders={"persian_label": sorted_labels, "sentiment_label": sentiment_order},
        title=title,
        labels={"persian_label": "Date (Shamsi)", "percentage": "% of tweets", "sentiment_label": "Sentiment"},
    )
    fig.update_layout(
        yaxis_ticksuffix="%",
        legend_title_text="Sentiment",
        template="plotly_white",
        xaxis_tickangle=45,
    )
    html_path = docs_dir / f"{filename_base}.html"
    fig.write_html(str(html_path), include_plotlyjs="cdn")
    print(f"Saved interactive chart -> {html_path}")

    # --- Matplotlib (static PNG) ---
    pivot = df.pivot(index="persian_label", columns="sentiment_label", values="percentage").fillna(0)
    pivot = pivot.sort_index()  # zero-padded labels sort correctly as plain text
    pivot = pivot.reindex(columns=[c for c in LABEL_ORDER if c in pivot.columns])

    fig_mpl, ax = plt.subplots(figsize=(12, 5))
    pivot.plot.area(ax=ax, color=[COLOR_MAP[c] for c in pivot.columns], alpha=0.85)
    ax.set_title(title)
    ax.set_xlabel("Date (Shamsi)")

    # Show at most `max_ticks` evenly-spaced x labels, instead of cramming
    # every single bucket onto the axis.
    n_ticks = len(pivot.index)
    step = max(1, n_ticks // max_ticks)
    tick_positions = range(0, n_ticks, step)
    ax.set_xticks(list(tick_positions))
    ax.set_xticklabels([pivot.index[i] for i in tick_positions], rotation=45, ha="right")

    ax.set_ylabel("% of tweets")
    ax.yaxis.set_major_formatter(lambda y, _: f"{y:.0f}%")
    ax.legend(title="Sentiment", bbox_to_anchor=(1.02, 1), loc="upper left")
    fig_mpl.tight_layout()

    png_path = PROJECT_ROOT / f"{filename_base}.png"
    fig_mpl.savefig(str(png_path), dpi=150)
    plt.close(fig_mpl)
    print(f"Saved static image -> {png_path}")


# ---------------------------------------------------------------
# Chart 1: hourly, last 10 days
# ---------------------------------------------------------------
hourly_df = load_data("toStartOfHour(send_time)", "hour_bucket")
print(f"Hourly: {len(hourly_df)} rows, {hourly_df['hour_bucket'].nunique() if not hourly_df.empty else 0} distinct hours.")
build_and_export(
    hourly_df,
    bucket_col="hour_bucket",
    persian_fmt="%Y/%m/%d %H:%M",
    title=f"Sahamyab Tweet Sentiment - Hourly (last {DAYS_TO_SHOW} days)",
    filename_base="sentiment_chart_hourly",
)

# ---------------------------------------------------------------
# Chart 2: daily, last 10 days
# ---------------------------------------------------------------
daily_df = load_data("toDate(send_time)", "date_bucket")
print(f"Daily: {len(daily_df)} rows, {daily_df['date_bucket'].nunique() if not daily_df.empty else 0} distinct days.")
build_and_export(
    daily_df,
    bucket_col="date_bucket",
    persian_fmt="%Y/%m/%d",
    title=f"Sahamyab Tweet Sentiment - Daily (last {DAYS_TO_SHOW} days)",
    filename_base="sentiment_chart_daily",
)

print("\nDone. Next steps:")
print("  1. Embed sentiment_chart_hourly.png and/or sentiment_chart_daily.png in README.md")
print("  2. git add docs/ sentiment_chart_hourly.png sentiment_chart_daily.png")
print("  3. Enable GitHub Pages: Settings -> Pages -> Source: main, folder /docs")
