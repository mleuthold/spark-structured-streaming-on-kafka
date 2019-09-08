import logging
import os

from pyspark.sql.types import StructType, StringType

from structured_streaming import StructuredStreamingApp

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def test_spark_session_dataframe(spark_session):
    """ test that a single event is parsed correctly """
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")

    results = test_df
    expected_results = test_df

    assert results == expected_results


def test_word_count(spark_session):
    """ test the word count """
    # input_df = spark_session.createDataFrame(["abc", "efg", "abc"], "x: string")

    input_path = f"file://{os.getcwd()}/tests/*.txt"
    logging.info(f"input_path: {input_path}")

    schema = StructType().add("value", StringType())

    input_df = spark_session.readStream.option("header", "false").schema(schema).csv(
        input_path)

    data = StructuredStreamingApp.transform_stream(input_df)
    data = StructuredStreamingApp.transform_stream_2(data)

    data.writeStream \
        .format("memory") \
        .queryName("Output") \
        .outputMode("complete") \
        .start() \
        .processAllAvailable()

    results = spark_session.sql("select * from Output").collect()
    logging.info(f"results: {results}")

    expected_results = spark_session.createDataFrame([('abc', 2), ('aaa', 1), ('efg', 1), ('words', 1), ('xyz', 1)],
                                                     ['word', 'count']).collect()
    logging.info(f"expected_results: {expected_results}")

    assert results == expected_results
