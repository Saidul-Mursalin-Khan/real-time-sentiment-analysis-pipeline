# # Use bullseye (Debian 11) explicitly — it has openjdk-11 which Flink 1.17.1 requires.
# # Trixie (Debian 13) dropped Java 11/17 and Flink 1.17.1 is incompatible with Java 21.
# FROM --platform=linux/arm64 python:3.10-slim-bullseye

# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#         wget curl openjdk-11-jdk-headless gcc g++ && \
#     rm -rf /var/lib/apt/lists/*

# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# WORKDIR /opt/flink-job

# COPY requirements.txt .

# # Install numpy FIRST at a pinned version to avoid ABI conflicts.
# # apache-flink 1.17.1 requires numpy<2.
# RUN pip install --no-cache-dir numpy==1.24.4 && \
#     pip install --no-cache-dir --timeout=300 apache-flink==1.17.1 && \
#     pip install --no-cache-dir --timeout=120 \
#         joblib==1.4.2 \
#         scikit-learn==1.3.2 \
#         nltk==3.8.1 \
#         psycopg2-binary==2.9.9

# # Download the Kafka SQL connector jar required by PyFlink
# RUN PYFLINK_LIB=$(python -c "import pyflink, os; print(os.path.dirname(pyflink.__file__))") && \
#     wget -q -P "$PYFLINK_LIB/lib/" \
#     https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

# # NLTK stopwords are downloaded at runtime inside MapFunction.open()

# COPY sentiment_job.py .

# CMD ["python", "sentiment_job.py"]

# -----

# FROM flink:1.17.1-scala_2.12-java11

# # Install Python
# RUN apt-get update && \
#     apt-get install -y python3 python3-pip wget openjdk-11-jdk && \
#     ln -s /usr/bin/python3 /usr/bin/python

# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# # Set working directory
# WORKDIR /opt/flink-job

# # Copy requirements
# COPY requirements.txt .

# # Install dependencies (order matters)
# RUN pip3 install --no-cache-dir numpy==1.24.4 && \
#     pip3 install --no-cache-dir apache-flink==1.17.1 && \
#     pip3 install --no-cache-dir \
#         joblib==1.4.2 \
#         scikit-learn==1.3.2 \
#         nltk==3.8.1 \
#         psycopg2-binary==2.9.9

# # Add Kafka connector
# RUN wget -q -P /opt/flink/lib/ \
#     https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

# # Copy job
# COPY sentiment_job.py .

# eclipse-temurin:11-jdk ships the FULL JDK (includes jni.h) on both
# amd64 and arm64 with no apt needed for Java at all.
# Ubuntu Jammy (22.04) mirrors are reliable — no Bullseye timeout issues.
FROM eclipse-temurin:11-jdk-jammy

# ── Python 3.10 + build tools ────────────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.10 python3.10-dev python3-pip \
        wget curl gcc g++ && \
    ln -sf /usr/bin/python3.10 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.10 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# JAVA_HOME is already set correctly by the temurin base image
# (typically /opt/java/openjdk — pemja will find jni.h there)

# ── Flink binary ─────────────────────────────────────────────────────────────
ENV FLINK_VERSION=1.17.1
ENV FLINK_HOME=/opt/flink

RUN wget -q "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz" \
        -O /tmp/flink.tgz && \
    tar -xzf /tmp/flink.tgz -C /opt && \
    mv /opt/flink-${FLINK_VERSION} ${FLINK_HOME} && \
    rm /tmp/flink.tgz

ENV PATH="${FLINK_HOME}/bin:${PATH}"

# ── Flink config — bind to all interfaces so UI is reachable from host ────────
RUN echo "jobmanager.rpc.address: flink-jobmanager" > ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "jobmanager.rpc.port: 6123" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "jobmanager.bind-host: 0.0.0.0" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "jobmanager.memory.process.size: 1600m" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "rest.address: 0.0.0.0" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "rest.bind-address: 0.0.0.0" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "rest.port: 8081" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "taskmanager.bind-host: 0.0.0.0" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "taskmanager.host: flink-taskmanager" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "taskmanager.numberOfTaskSlots: 4" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "taskmanager.memory.process.size: 1728m" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "parallelism.default: 1" >> ${FLINK_HOME}/conf/flink-conf.yaml && \
    echo "rest.flamegraph.enabled: true" >> ${FLINK_HOME}/conf/flink-conf.yaml

# ── Kafka SQL connector jar ───────────────────────────────────────────────────
RUN wget -q -P ${FLINK_HOME}/lib/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar"

# ── Python deps ───────────────────────────────────────────────────────────────
WORKDIR /opt/flink-job

COPY requirements.txt .

# numpy must be pinned <2 and installed BEFORE apache-flink
# apache-flink pulls in pemja which compiles JNI — needs the JDK above
RUN pip3 install --no-cache-dir numpy==1.24.4 && \
    pip3 install --no-cache-dir --timeout=300 apache-flink==1.17.1 && \
    pip3 install --no-cache-dir --timeout=120 \
        joblib==1.4.2 \
        scikit-learn==1.3.2 \
        nltk==3.8.1 \
        psycopg2-binary==2.9.9

# ── Job script ────────────────────────────────────────────────────────────────
COPY sentiment_job.py .

CMD ["python", "sentiment_job.py"]