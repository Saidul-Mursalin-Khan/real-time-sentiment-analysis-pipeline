
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaRecordSerializationSchema, KafkaOffsetsInitializer, KafkaSink
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction
from collections import OrderedDict
from nltk.stem import SnowballStemmer
import os
import re
import json


class InferMapper(MapFunction):

    def __init__(self):
        self.model_dir = os.environ.get("MODEL_DIR", "/models")
        self.cache_size = int(os.environ.get("MODEL_CACHE_SIZE", "10"))

    def open(self, runtime_context):
        import joblib
        self._joblib = joblib
        self.cache = OrderedDict()
        self.stemmer = SnowballStemmer("english")
        self._emoji_re = re.compile(u"[\U0001F000-\U0001FFFF]")

    def map(self, value):
        req = json.loads(value)
        keyword = req.get("keyword")
        event_time = req.get("event_time")

        if not keyword or event_time is None:
            print("Inference failed: request missing keyword/event_time", flush=True)
            return json.dumps({**req, "available": False, "error": "bad request"})

        bundle = self.get_model(event_time)
        if bundle is None:
            return json.dumps({**req, "available": False})

        vec, clf = bundle["vectorizer"], bundle["classifier"]
        X = vec.transform([self.preprocess(keyword)])

        probabilities = clf.predict_proba(X)[0]
        classes = clf.classes_

        proba = {}
        for c, p in zip(classes, probabilities):
            proba[str(c)] = float(p)

        label = str(classes[probabilities.argmax()])

        response = {
            "request_id": req.get("request_id"),
            "keyword": keyword,
            "event_time": event_time,
            "label": label,
            "proba": proba,
        }
        return json.dumps(response)

    def get_model(self, event_time):
        if event_time in self.cache:
            self.cache.move_to_end(event_time)
            return self.cache[event_time]

        path = f"{self.model_dir}/{event_time}.pkl"
        if not os.path.exists(path):
            return None

        bundle = self._joblib.load(path)
        self.cache[event_time] = bundle
        if len(self.cache) > self.cache_size:
            self.cache.popitem(last=False)

        return bundle
    
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
            # Stop-word filtering intentionally disabled.
            else:
                result.append(self.stemmer.stem(tok))
        return " ".join(result)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    parallelism = int(os.environ.get("INFERENCE_PARALLELISM", "2"))
    print(f"Inference job start: parallelism={parallelism}", flush=True)
    env.set_parallelism(parallelism)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("inference-request")
        .set_group_id("inference_group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    ds = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource",
    )

    ds = ds.map(InferMapper(), output_type=Types.STRING())

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_property("enable.idempotence", "false")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("inference-response")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    ds.sink_to(sink)
    env.execute("inference")


if __name__ == "__main__":
    main()
