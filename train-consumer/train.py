from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
import pandas as pd
from kafka import KafkaConsumer,KafkaProducer
import json
import joblib
import os
import time
import glob
from collections import deque
# --- DB-backed evaluation disabled (oracle F1 → model_versions table) — meaningless for now ---
# from sklearn.metrics import f1_score
# import psycopg2

BATCH_SIZE = 20000
WINDOW_SIZE = 10
MODEL_DIR   = os.environ.get("MODEL_DIR", "/models")
KEEP_LAST_K_MODELS = int(os.environ.get("KEEP_LAST_K_MODELS", "10"))
# ORACLE_PATH = os.environ.get("ORACLE_PATH", "/app/oracle_dataset_clean.csv")
# DB_HOST     = os.environ.get("DB_HOST", "postgres")
# DB_NAME     = os.environ.get("DB_NAME", "sentiment")
# DB_USER     = os.environ.get("DB_USER", "sentiment")
# DB_PASS     = os.environ.get("DB_PASS", "sentiment")


# --- DB-backed evaluation disabled — meaningless for now ---
# def init_db():
#     conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
#     cur = conn.cursor()
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS model_versions (
#             version_id VARCHAR(50) PRIMARY KEY,
#             created_at TIMESTAMP DEFAULT NOW(),
#             f1_score FLOAT
#         );
#     """)
#     conn.commit()
#     cur.close()
#     conn.close()
#
# def save_evaluation(version, f1):
#     conn = psycopg2.connect(
#         host=DB_HOST,
#         dbname=DB_NAME,
#         user=DB_USER,
#         password=DB_PASS
#     )
#
#     cur = conn.cursor()
#     cur.execute("""
#         INSERT INTO model_versions (
#                 version_id,
#                 created_at,
#                 f1_score)
#             VALUES (
#                 %s, NOW(), %s)
#         """,
#         (version, f1)
#     )
#     conn.commit()
#     cur.close()
#     conn.close()


def train(train_set):

    df_train=pd.DataFrame(train_set)
    df_train = df_train[df_train["label"] != "neutral"].reset_index(drop=True)
    print(f"Binary training set: {len(df_train)} samples "
          f"(positive: {(df_train['label']=='positive').sum()}, "
          f"negative: {(df_train['label']=='negative').sum()})", flush=True)
    
    vectorizer = TfidfVectorizer(max_features=50000)

    x_train=vectorizer.fit_transform(df_train["body"])

    y_train=df_train["label"]

    model = LogisticRegression(class_weight='balanced', max_iter=1000)
    model.fit(x_train, y_train)
    
    event_time = int(pd.to_numeric(df_train["created_utc"], errors="coerce").max())
  

    tmp_path = f"{MODEL_DIR}/{event_time}.pkl.tmp"
    final_path = f"{MODEL_DIR}/{event_time}.pkl"
    joblib.dump({'vectorizer': vectorizer, 'classifier': model}, tmp_path)
    os.replace(tmp_path, final_path)
    print(f"Model {event_time} trained and saved ({len(train_set)} samples)", flush=True)
    
    remove_old_models()
    
    return event_time
    
    
    
    
    
    

    # --- DB-backed F1 evaluation disabled — meaningless for now ---
    # df_test=pd.read_csv(ORACLE_PATH)
    # x_test=vectorizer.transform(df_test["body"])
    # y_test=df_test["label"]
    # y_pred=model.predict(x_test)
    # f1=f1_score(y_test, y_pred,average="macro")
    # save_evaluation(version,f1)
def event_time_of(path):
    name = os.path.basename(path)
    return int(name.split(".")[0])


def remove_old_models(k=KEEP_LAST_K_MODELS):
    # only numeric-named pkls are versioned models; skip anything else
    files = [f for f in glob.glob(f"{MODEL_DIR}/*.pkl")
             if os.path.basename(f).split(".")[0].isdigit()]
    files.sort(key=event_time_of)
    for path in files[:-k]:
        os.remove(path)
        
    
    
    



def main():
    # init_db()  # DB-backed evaluation disabled — meaningless for now
    from kafka.errors import NoBrokersAvailable
    while True:
        try:
            consumer = KafkaConsumer(
                "labeled-train-data",
                bootstrap_servers="kafka:9092",
                group_id="trainer-group",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                auto_offset_reset="earliest"
            )
            break
        except NoBrokersAvailable:
            print("Kafka not ready, retrying in 5s...")
            time.sleep(5)
    
    
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    
    

    
    window=deque(maxlen=WINDOW_SIZE)
    batch_buffer=[]
    for msg in consumer:
        batch_buffer.append(msg.value)
        
        if len(batch_buffer)>=BATCH_SIZE:
            window.append(batch_buffer)
            batch_buffer=[]
            
            #start training when the window is full
            if len(window)>= WINDOW_SIZE:
                train_set=[item for batch in window for item in batch ]
                event_time=train(train_set)
                producer.send("model-version", value={"event_time": event_time})
                producer.flush()
                
    
    


if __name__ =="__main__":
    main()
