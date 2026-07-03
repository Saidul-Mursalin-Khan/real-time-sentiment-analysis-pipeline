"""
sentiment_job.py
Flink pipeline:
  Kafka source
    → PreprocessMapper   (lowercase, URL strip, emoji-safe, stem, stopwords)
    → SentimentMapper    (TF-IDF + LogisticRegression from joblib)
    → KeywordRouter      (routes to matched-keywords output)
    → PostgresSink       (writes results to PostgreSQL)
"""
  

import re
import json
import os
import logging

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction, FilterFunction

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("sentiment_job")

KEYWORDS = [kw.strip().lower() for kw in os.environ.get("KEYWORDS", "python,java").split(",")]

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "sentiment")
DB_USER = os.environ.get("DB_USER", "sentiment")
DB_PASS = os.environ.get("DB_PASS", "sentiment")

# ---------------------------------------------------------------------------
# Stage 1 — Text Preprocessing (must live in Flink, not in producer)
# ---------------------------------------------------------------------------
class PreprocessMapper(MapFunction):
    """
    Cleans raw Reddit comment bodies:
      - lowercase
      - strip URLs
      - remove punctuation / numbers while PRESERVING emoji codepoints
      - remove English stop-words
      - apply Snowball stemming to ASCII tokens (emojis kept as-is)
    """

    def open(self, runtime_context):
        import nltk
        from nltk.corpus import stopwords
        from nltk.stem import SnowballStemmer

        nltk.download("stopwords", quiet=True)
        self.stemmer = SnowballStemmer("english")
        self.stop_words = set(stopwords.words("english"))
        # Emoji range: U+1F000 – U+1FFFF (covers most common emoji blocks)
        self._emoji_re = re.compile(u"[\U0001F000-\U0001FFFF]")

    def preprocess(self, text: str) -> str:
        text = text.lower()
        # Remove URLs
        text = re.sub(r"https?://\S+|www\.\S+", "", text)
        # Keep: a-z, whitespace, /r/ subreddit refs, and emoji codepoints
        # We do this token-by-token to avoid clobbering multi-byte emoji
        cleaned_chars = []
        for ch in text:
            if ch.isalpha() or ch.isspace() or ch == "/" or self._emoji_re.match(ch):
                cleaned_chars.append(ch)
            else:
                cleaned_chars.append(" ")
        text = "".join(cleaned_chars)

        tokens = text.split()
        result = []
        for tok in tokens:
            if self._emoji_re.search(tok):
                # Preserve emoji tokens unchanged
                result.append(tok)
            elif tok not in self.stop_words:
                result.append(self.stemmer.stem(tok))
        return " ".join(result)

    def map(self, value: str) -> str:
        try:
            obj = json.loads(value)
            body = obj.get("body", "")
            obj["cleaned_body"] = self.preprocess(body)
            return json.dumps(obj)
        except Exception as exc:
            return json.dumps({"error": str(exc), "raw": value[:120]})


# ---------------------------------------------------------------------------
# Stage 2 — Sentiment Classification
# ---------------------------------------------------------------------------
class SentimentMapper(MapFunction):

    RELOAD_EVERY = 1000  # check for new model every 1000 records

    def open(self, runtime_context):
        import joblib, glob
        self._joblib = joblib
        self._glob = glob
        self._count = 0
        self._clf_path = None  # track currently loaded model path
        self._load_latest()

    def _load_latest(self):
        clf_files = sorted(self._glob.glob("/model/sentiment_model_v*.joblib"))
        clf_path = clf_files[-1] if clf_files else "/model/sentiment_model.joblib"
        vec_files = sorted(self._glob.glob("/model/tfidf_vectorizer_v*.joblib"))
        vec_path = vec_files[-1] if vec_files else "/model/tfidf_vectorizer.joblib"

        # Only reload if the file actually changed — avoids unnecessary disk I/O
        if self._clf_path == clf_path:
            return

        log.info("Loading model: %s | vectorizer: %s", clf_path, vec_path)  # fixed: was [log.info](http://log.info)
        self.clf = self._joblib.load(clf_path)
        self.vectorizer = self._joblib.load(vec_path)
        self._clf_path = clf_path

    def map(self, value: str) -> str:
        self._count += 1
        if self._count % self.RELOAD_EVERY == 0:
            self._load_latest()  # hot-swap model if retrainer wrote a new version

        try:
            obj = json.loads(value)
            if "error" in obj:
                return value
            cleaned = obj.get("cleaned_body", "")
            vec = self.vectorizer.transform([cleaned])
            label = self.clf.predict(vec)[0]
            proba = self.clf.predict_proba(vec)[0]
            score = float(max(proba))
            obj["sentiment"] = label
            obj["sentiment_score"] = round(score, 4)
            return json.dumps(obj)
        except Exception as exc:
            return json.dumps({"error": str(exc), "raw": value[:120]})


# ---------------------------------------------------------------------------
# Stage 3 — Keyword Routing
# ---------------------------------------------------------------------------
class KeywordRouter(MapFunction):
    """
    Adds 'matched_keywords' list. Records without any match still pass through
    so the sink can track total volume (useful for retraining triggers).
    """

    def __init__(self, keywords):
        self.keywords = keywords

    def map(self, value: str) -> str:
        try:
            obj = json.loads(value)
            cleaned = obj.get("cleaned_body", "")
            matched = [kw for kw in self.keywords if kw in cleaned]
            obj["matched_keywords"] = matched
            return json.dumps(obj)
        except Exception as exc:
            return json.dumps({"error": str(exc), "raw": value[:120]})


class HasKeywordFilter(FilterFunction):
    """Only forward records that matched at least one keyword."""

    def filter(self, value: str) -> bool:
        try:
            obj = json.loads(value)
            return bool(obj.get("matched_keywords"))
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Stage 4 — PostgreSQL Sink
# ---------------------------------------------------------------------------
class PostgresSink(MapFunction):
    """
    Writes processed + keyword-matched records to PostgreSQL.
    Uses a simple upsert so re-runs are idempotent.
    Also implements a retraining trigger: every RETRAIN_EVERY rows it writes
    a flag to the `retrain_signals` table that the training job polls.
    """

    RETRAIN_EVERY = int(os.environ.get("RETRAIN_EVERY", 50000))

    def open(self, runtime_context):
        import psycopg2
        self._conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        self._conn.autocommit = False
        self._cur = self._conn.cursor()
        self._buf = []
        self._total = 0
        self._ensure_schema()

    def _ensure_schema(self):
        self._cur.execute("""
            CREATE TABLE IF NOT EXISTS sentiment_results (
                id              TEXT PRIMARY KEY,
                author          TEXT,
                subreddit       TEXT,
                created_utc     BIGINT,
                score           INT,
                controversiality INT,
                body            TEXT,
                cleaned_body    TEXT,
                sentiment       TEXT,
                sentiment_score FLOAT,
                matched_keywords TEXT[],
                processed_at    TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS retrain_signals (
                id          SERIAL PRIMARY KEY,
                triggered_at TIMESTAMPTZ DEFAULT NOW(),
                rows_processed BIGINT,
                done        BOOLEAN DEFAULT FALSE
            );
        """)
        self._conn.commit()

    def map(self, value: str) -> str:
        try:
            obj = json.loads(value)
            if "error" in obj:
                return value

            self._buf.append(obj)
            if len(self._buf) >= 500:
                self._flush()
        except Exception as exc:
            log.warning("PostgresSink map error: %s", exc)
        return value  # pass through for result.print() debug

    def _flush(self):
        if not self._buf:
            return
        rows = []
        for obj in self._buf:
            rows.append((
                obj.get("id", ""),
                obj.get("author", ""),
                obj.get("subreddit", ""),
                obj.get("created_utc"),
                obj.get("score"),
                obj.get("controversiality"),
                obj.get("body", ""),
                obj.get("cleaned_body", ""),
                obj.get("sentiment", ""),
                obj.get("sentiment_score", 0.0),
                obj.get("matched_keywords", []),
            ))
        self._cur.executemany("""
            INSERT INTO sentiment_results
                (id, author, subreddit, created_utc, score, controversiality,
                 body, cleaned_body, sentiment, sentiment_score, matched_keywords)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                sentiment       = EXCLUDED.sentiment,
                sentiment_score = EXCLUDED.sentiment_score,
                matched_keywords= EXCLUDED.matched_keywords,
                processed_at    = NOW()
        """, rows)
        self._conn.commit()

        self._total += len(self._buf)
        self._buf.clear()

        # Retraining trigger
        if self._total % self.RETRAIN_EVERY < 500:
            self._cur.execute(
                "INSERT INTO retrain_signals (rows_processed) VALUES (%s)",
                (self._total,)
            )
            self._conn.commit()
            log.info("Retraining signal inserted at %d rows", self._total)

    def close(self):
        self._flush()
        self._cur.close()
        self._conn.close()


# ---------------------------------------------------------------------------
# Flink Pipeline Assembly
# ---------------------------------------------------------------------------
def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("reddit-comments")
        .set_group_id("flink-sentiment-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="KafkaRedditComments",
    )

    # Stage 1: Preprocessing (Flink operator — not in producer!)
    preprocessed = raw_stream.map(
        PreprocessMapper(), output_type=Types.STRING()
    ).name("TextPreprocessing")

    # Stage 2: Sentiment classification
    with_sentiment = preprocessed.map(
        SentimentMapper(), output_type=Types.STRING()
    ).name("SentimentClassifier")

    # Stage 3: Keyword routing
    with_keywords = with_sentiment.map(
        KeywordRouter(KEYWORDS), output_type=Types.STRING()
    ).name("KeywordRouter")

    # Stage 4: Save ALL records to PostgreSQL (counts all rows for retrain trigger)
    with_keywords.map(PostgresSink(), output_type=Types.STRING()).name("PostgresSink")
    # Stage 4b: Separate debug log for keyword-matched records only
    keyword_hits = with_keywords.filter(HasKeywordFilter()).name("KeywordFilter")

    # Stage 4b: Debug logger (avoids Java 21 reflection issue caused by .print())
    class DebugLogger(MapFunction):
        def map(self, value):
            try:
                obj = json.loads(value)
                log.info(
                    "[DEBUG] id=%s subreddit=%s sentiment=%s score=%.2f keywords=%s",
                    obj.get("id", "?"),
                    obj.get("subreddit", "?"),
                    obj.get("sentiment", "?"),
                    obj.get("sentiment_score", 0.0),
                    obj.get("matched_keywords", []),
                )
            except Exception:
                pass
            return value

    with_keywords.map(DebugLogger(), output_type=Types.STRING()).name("DebugLogger")

    return env


if __name__ == "__main__":
    log.info("Starting Reddit Sentiment Flink Job | keywords=%s", KEYWORDS)
    env = build_pipeline()
    env.execute("Reddit Sentiment Analysis")