import os
import nltk
import json
import pandas as pd
from pyspark.sql import SparkSession
from kafka import KafkaProducer
from pyspark.sql.functions import udf, explode, col, current_timestamp, window
from pyspark.sql.types import ArrayType, StringType
from nltk import word_tokenize, pos_tag, ne_chunk

# Download necessary NLTK models
nltk.download("punkt")
nltk.download("averaged_perceptron_tagger")
nltk.download("maxent_ne_chunker")
nltk.download("words")

def extract_named_entities(json_text: str) -> list:
    """
    Function to extract named entities from text
    """

    try:
        data = json.loads(json_text)
        text = data.get("body", "")
    except Exception:
        text = json_text

    tokens = word_tokenize(text)
    tags = pos_tag(tokens)
    chunks = ne_chunk(tags, binary=False)

    entities = []
    for subtree in chunks.subtrees():
        if subtree.label() != "S":  # skip root
            entity = " ".join(word for word, tag in subtree.leaves())
            entities.append(entity)
    return entities

# Initialize Kafka producer     
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def stream_to_kafka(batch_df, batch_id):
    """
    Function to send top entities per batch to Kafka
    """
    top_entities = (
        batch_df.orderBy(col("count").desc())
        .limit(10)
        .select("entity", "count")
        .toPandas()
        .to_dict(orient="records")
    )

    message = {
        "batch_id": batch_id,
        "top_entities": top_entities
    }

    print(f"[Batch {batch_id}] message: {message}") 
    producer.send("reddit-entities", value=message)
    producer.flush()
    print(f"[Batch {batch_id}] Sent top 10 entities to Kafka topic 'reddit-entities'")

if __name__ == "__main__":
    # Initialize Spark session
    spark = (
        SparkSession.builder
        .appName("Reddit-Stream-NER")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Register UDF
    extract_ner_udf = udf(extract_named_entities, ArrayType(StringType()))

    # Consume Reddit stream from Kafka
    messages = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "reddit-stream")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as value")
    )

    # Apply NER
    ner_df = messages.withColumn("entities", extract_ner_udf(col("value")))

    # Add timestamp column
    ner_df = ner_df.withColumn("timestamp", current_timestamp())

    # Explode entities
    exploded_df = ner_df.select(explode(col("entities")).alias("entity"), "timestamp")

    # Count entities in 5-minute tumbling windows
    entity_counts = (
        exploded_df
        .withWatermark("timestamp", "1 hour")
        .groupBy(window("timestamp", "5 minutes"), col("entity"))
        .count()
    )

    # Write stream: console send to Kafka per batch
    query = (
        entity_counts.writeStream
        .outputMode("complete")
        .foreachBatch(stream_to_kafka)
        .trigger(processingTime="5 minutes")
        .start()
    )

    query.awaitTermination()
