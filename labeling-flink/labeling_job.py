
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource,KafkaRecordSerializationSchema, KafkaOffsetsInitializer,KafkaSink
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction, FilterFunction
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()


class LabelingMapper(MapFunction):
    def map(self, value:str):
        comment=json.loads(value)
        comment_body=comment.get("body","")
        result=v_label_sentiment(comment_body)
        if result is None:
            return value
        label,score=result
        comment["label"]=label
        comment["score"]=score
        
        return json.dumps(comment)
    


def v_label_sentiment(text,p_threshold=0.37,n_threshold=-0.01):
    if not isinstance(text, str):
        return None
    score = analyzer.polarity_scores(text)['compound']
    if score > p_threshold:
        return ['positive',score]
    elif score < n_threshold:
        return ['negative',score]
    else:
        return ["neutral",score ]
    



def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("cleaned-comments")
        .set_group_id("labeling_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()

    )
    ds=(env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "KafkaSource")
    .set_parallelism(3)
    )
    ds=ds.map(LabelingMapper(), output_type=Types.STRING())
    
    sink = (
        KafkaSink.builder() 
        .set_bootstrap_servers("kafka:9092") 
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_property("enable.idempotence", "false")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("labeled-train-data")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            ) 
    .build()
    )

    ds.sink_to(sink)
    env.execute("labeling")
    
if __name__ =="__main__":
    main()
