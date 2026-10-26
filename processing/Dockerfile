
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

# Driver JDBC để Spark ghi vào ClickHouse.
# Dùng 0.6.4: bản "-all" của 0.6.5 bị đóng gói thiếu class.
ADD --chmod=644 \
    ${MAVEN}/com/clickhouse/clickhouse-jdbc/0.6.4/clickhouse-jdbc-0.6.4-all.jar \
    /opt/spark/jars/

# Thư viện Python của ClickHouse: xoá dữ liệu theo ngày và chèn bảng nhỏ từ driver.
RUN pip install --no-cache-dir clickhouse-connect==0.8.11

# pyspark.ml cần numpy; hàm vector_to_array của pyspark.ml.functions
# cần thêm pandas và pyarrow.
RUN pip install --no-cache-dir \
    numpy==1.26.4 \
    pandas==2.2.3 \
    pyarrow==17.0.0

# Tạo thư mục làm việc và cấp quyền cho user spark.
RUN mkdir -p /opt/spark/checkpoints /opt/spark/work /opt/gdelt/jobs \
    && chown -R spark:spark /opt/spark/checkpoints /opt/spark/work /opt/gdelt

ENV PYTHONPATH=/opt/gdelt/jobs \
    PYTHONUNBUFFERED=1 \
    SPARK_WORKER_DIR=/opt/spark/work \
    TZ=UTC
    
COPY --chown=spark:spark jobs /opt/gdelt/jobs
COPY --chown=spark:spark lookups /opt/gdelt/lookups

USER spark
WORKDIR /opt/gdelt/jobs
