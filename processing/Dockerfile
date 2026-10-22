FROM apache/spark:3.5.6-java17-python3

USER root

# Thư viện Java nạp sẵn vào image để spark-submit không phải tải lại mỗi lần chạy:
#   - Kafka source cho Structured Streaming (+ 3 thư viện nó phụ thuộc)
#   - hadoop-aws + AWS SDK để Spark đọc/ghi s3a://
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.6/spark-token-provider-kafka-0-10_2.12-3.5.6.jar \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/jars/

RUN mkdir -p /opt/spark/checkpoints /opt/spark/work /opt/gdelt/jobs \
    && chown -R spark:spark /opt/spark/checkpoints /opt/spark/work /opt/gdelt

ENV PYTHONPATH=/opt/gdelt/jobs \
    PYTHONUNBUFFERED=1 \
    SPARK_WORKER_DIR=/opt/spark/work \
    TZ=UTC

USER spark
WORKDIR /opt/gdelt/jobs


