import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

def configure_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)


def create_spark():
    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()
    spark.conf.set("spark.sql.streaming.checkpointLocation", "file:///tmp")
    return spark


def read_stream_socket(spark):
    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    return lines


def read_stream_kafka(spark):
    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "input-topic") \
        .load()
    lines = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    return lines

def transform_stream(data):
    # Split the lines into words
    words = data.select(
        explode(
            split(data.value, " ")
        ).alias("word")
    )
    return words


def transform_stream_2(data):
    # Generate running word count
    wordCounts = data.groupBy("word").count()
    return wordCounts


def write_stream_console(data):
    # Start running the query that prints the running counts to the console
    query = data \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option('truncate', 'false') \
        .start()
    return query

def write_stream_memory(data):
    # Start running the query that prints the running counts to the console
    query = data \
        .writeStream \
        .format("memory") \
        .queryName("Output") \
        .outputMode("complete") \
        .start()
    return query

def write_stream_kafka(data):
    # Start running the query that prints the running counts to the console
    query = data \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .outputMode("append") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "output-topic") \
        .start()
    return query

def logic_main(spark):
    data = read_stream_kafka(spark)

    print(data.isStreaming)
    print(data.printSchema())

    # data = transform_stream(data)
    # data = transform_stream_2(data)
    query = write_stream_kafka(data)

    query.awaitTermination()


def my_application_main():
    configure_logging()
    spark = create_spark()
    logic_main(spark)


if __name__ == "__main__":
    my_application_main()
