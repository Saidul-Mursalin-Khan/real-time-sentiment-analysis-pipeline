import os
import json
import time
import uuid
import threading
from collections import deque, defaultdict

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from fastapi.staticfiles import StaticFiles

# ── Config ───
BROKER        = os.environ.get("KAFKA_BROKER", "kafka:9092")
KEEP_VERSIONS = int(os.environ.get("KEEP_VERSIONS", "50"))

# ── App ───
app = FastAPI(title="Reddit Sentiment Backend")
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Shared in-memory state ───
_lock = threading.Lock()
active_keywords: set[str]           = set()
recent_versions: deque[int]         = deque(maxlen=KEEP_VERSIONS)  # event_times seen
series: dict[str, list[dict]]       = defaultdict(list)            # keyword -> [{event_time, label, proba}]
seen_points: set[tuple[str, int]]   = set()                        # (keyword, event_time) dedup

# pending one-shot requests: request_id -> threading.Event + result holder
_pending: dict[str, dict] = {}


# ── Kafka helpers ───────────
def make_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print("Kafka not ready (producer), retrying in 5s...", flush=True)
            time.sleep(5)

def make_consumer(topic: str, offset: str) -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=BROKER,
                group_id=f"backend-{topic}-{uuid.uuid4()}",
                auto_offset_reset=offset,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready (consumer {topic}), retrying in 5s...", flush=True)
            time.sleep(5)


producer = make_producer()


# def send_inference_request(keyword: str, event_time: int, request_id: str = None) -> str:
#     rid = request_id or str(uuid.uuid4())
#     producer.send("inference-request", value={
#         "request_id": rid,
#         "keyword": keyword,
#         "model_version": event_time,
#     })
#     producer.flush()
#     return rid


def send_inference_request(keyword: str, event_time: int, request_id: str = None) -> str:
    rid = request_id or str(uuid.uuid4())

    producer.send("inference-request", value={
        "request_id": rid,
        "keyword": keyword,
        "event_time": event_time
    })

    producer.flush()
    return rid


# ── Background threads ───────
def model_version_loop() -> None:
    consumer = make_consumer("model-version", "earliest")
    print("model-version consumer started", flush=True)
    for msg in consumer:
        event_time = msg.value.get("event_time")
        if event_time is None:
            continue
        with _lock:
            if event_time not in recent_versions:
                recent_versions.append(event_time)
            keywords = list(active_keywords)
        for kw in keywords:
            send_inference_request(kw, event_time)
        print(f"model-version {event_time} → requested for keywords: {keywords}", flush=True)


def response_loop() -> None:
    consumer = make_consumer("inference-response", "latest")
    print("inference-response consumer started", flush=True)
    for msg in consumer:
        r = msg.value

        # skip unavailable responses
        if r.get("available") is False:
            continue

        keyword    = r.get("keyword")
        event_time = r.get("event_time")
        request_id = r.get("request_id")

        # resolve a pending one-shot /analyze request if it matches
        if request_id:
            with _lock:
                pending = _pending.get(request_id)
            if pending:
                pending["result"] = r
                pending["event"].set()
                continue   # don't add one-shot results to the series chart

        if keyword is None or event_time is None:
            continue

        # store in series for /series endpoint
        with _lock:
            key = (keyword, event_time)
            if key in seen_points:
                continue
            seen_points.add(key)
            series[keyword].append({
                "event_time": event_time,
                "label":      r.get("label"),
                "proba":      r.get("proba", {}),
            })
            series[keyword].sort(key=lambda p: p["event_time"])


@app.on_event("startup")
def startup() -> None:
    threading.Thread(target=model_version_loop, daemon=True).start()
    threading.Thread(target=response_loop,      daemon=True).start()
    print("Backend started. Kafka threads running.", flush=True)


# ── Request/Response models ───────────
class WatchPayload(BaseModel):
    keywords: list[str]

class AnalyzePayload(BaseModel):
    keyword: str = ""
    text: str = ""    


# ── Endpoints ───────────────

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

@app.get("/model/status")
def model_status() -> dict:
    with _lock:
        versions = list(recent_versions)
    if not versions:
        return {
            "available": False,
            "message": "No model trained!"
        }
    return {
        "available":       True,
        "total_versions":  len(versions),
        "latest_version":  max(versions),
    }


@app.post("/watch")
def watch(payload: WatchPayload) -> dict:
    keywords = [k.strip() for k in payload.keywords if k.strip()]
    if not keywords:
        raise HTTPException(status_code=400, detail="Provide at least one keyword.")

    with _lock:
        active_keywords.clear()
        active_keywords.update(keywords)
        versions = list(recent_versions)
        series.clear()
        seen_points.clear()

    # backfill — request inference for every past model version
    for kw in keywords:
        for event_time in versions:
            send_inference_request(kw, event_time)

    return {
        "watching":          keywords,
        "backfill_versions": len(versions),
    }


@app.get("/series")
def get_series() -> dict:
    with _lock:
        return {k: list(v) for k, v in series.items()}


@app.post("/analyze")
def analyze(payload: AnalyzePayload) -> dict:
    with _lock:
        versions = list(recent_versions)

    if not versions:
        raise HTTPException(
            status_code=503,
            detail="trained model unavailabel!"
        )

    latest_version = max(versions)
    request_id     = str(uuid.uuid4())
    event          = threading.Event()

    with _lock:
        _pending[request_id] = {"event": event, "result": None}

    # combine keyword + text so inference has more context
    # combined_keyword = f"{payload.keyword} {payload.text}".strip()
    combined_keyword = f"{payload.text}"

    send_inference_request(combined_keyword, latest_version, request_id)

    # wait up to 10 seconds for inference-flink to respond
    resolved = event.wait(timeout=10.0)

    with _lock:
        pending = _pending.pop(request_id, None)

    if not resolved or pending is None or pending["result"] is None:
        raise HTTPException(
            status_code=504,
            detail="Inference error!"
        )

    r = pending["result"]
    return {
        "keyword":    payload.keyword,
        "label":      r.get("label", "unknown"),
        "proba":      r.get("proba", {}),
        "event_time": r.get("event_time"),
    }


@app.get("/")
def index() -> FileResponse:
    
    return FileResponse(
        os.path.join(os.path.dirname(__file__), "static", "index.html")
    )
