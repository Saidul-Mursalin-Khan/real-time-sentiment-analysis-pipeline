
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource,KafkaRecordSerializationSchema, KafkaOffsetsInitializer,KafkaSink
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction, FilterFunction
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re

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
        from nltk.stem import SnowballStemmer

        self.stemmer = SnowballStemmer("english")
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
            # Stop-word filtering intentionally disabled.
            else:
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



    



def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("reddit-comments")
        .set_group_id("preprocess_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()

    )
    ds=(env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "KafkaSource")
    .set_parallelism(3)
    )
    ds= ds.map(
        PreprocessMapper(), output_type=Types.STRING()
    ).name("TextPreprocessing")
    sink = (
        KafkaSink.builder() 
        .set_bootstrap_servers("kafka:9092") 
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_property("enable.idempotence", "false")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("cleaned-comments")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            ) 
    .build()
    )

    ds.sink_to(sink)
    env.execute("labeling")
    
if __name__ =="__main__":
    main()
