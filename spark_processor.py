import spacy
from collections import Counter

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from config import INPUT_TOPIC, OUTPUT_TOPIC, KAFKA_BOOTSTRAP_SERVER


# Define the schema for named entities (text and count)
udf_schema = T.ArrayType(T.StructType([
    T.StructField("keyword", T.StringType(), True),
    T.StructField("count", T.IntegerType(), True)
]))

# Load spaCy model
nlp = spacy.load("en_core_web_sm")


def named_entity_recognition(text: str):
    doc = nlp(text)
    entity_counts = Counter(ent.text for ent in doc.ents)
    return list(entity_counts.items())


# Register the UDF
ner_udf = F.udf(named_entity_recognition, udf_schema)

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .master("local[3]") \
    .appName("NewsAPI_NER_counter") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Read from the input Kafka topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", INPUT_TOPIC)
    .load()
)

# perform NER
df = (
    df
    .selectExpr('CAST(value AS STRING)')
    .select(F.explode(ner_udf(F.col("value"))).alias("entity"))
    .select("entity.*")
    .groupBy("keyword")
    .agg(F.sum("count").alias("count"))
    .select(F.to_json(F.struct("keyword", "count")).alias("value"))
)

# df.show(truncate=False)

# Write to the output Kafka topic
query = (
    df.writeStream.format("kafka")
    .outputMode("update")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", "./ckpt")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
