FROM eclipse-temurin:11-jdk-jammy

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.10 python3.10-dev python3-pip \
        wget curl gcc g++ && \
    ln -sf /usr/bin/python3.10 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.10 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

ENV FLINK_VERSION=1.17.1
ENV FLINK_HOME=/opt/flink

RUN wget -q "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz" \
        -O /tmp/flink.tgz && \
    tar -xzf /tmp/flink.tgz -C /opt && \
    mv /opt/flink-${FLINK_VERSION} ${FLINK_HOME} && \
    rm /tmp/flink.tgz

ENV PATH="${FLINK_HOME}/bin:${PATH}"

RUN wget -q -P ${FLINK_HOME}/lib/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar"

WORKDIR /opt/inference-job

COPY requirements.txt .

RUN pip3 install --no-cache-dir --timeout=300 -r requirements.txt

COPY inference.py .

CMD ["python", "inference.py"]
