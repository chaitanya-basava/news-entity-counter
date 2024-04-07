import spacy
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from config import INPUT_TOPIC, OUTPUT_TOPIC, KAFKA_BOOTSTRAP_SERVER

# Define schema of input json
input_schema = T.StructType([
    T.StructField("author", T.StringType(), True),
    T.StructField("body", T.StringType(), True)
])

# Define the schema for named entities (text and count)
udf_schema = T.ArrayType(T.StructType([
    T.StructField("text", T.StringType(), True),
    T.StructField("count", T.IntegerType(), True)
]))

# Load spaCy model
nlp = spacy.load("en_core_web_sm")


def named_entity_recognition(text: str):
    """Performs named entity recognition on the input text."""
    doc = nlp(text)
    entities = {}
    for ent in doc.ents:
        entities[ent.text] = entities.get(ent.text, 0) + 1
    return [(ent, count) for ent, count in entities.items()]


# Register the UDF
ner_udf = F.udf(named_entity_recognition, udf_schema)

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .master("local[3]") \
    .appName("NERWithKafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Read from Kafka topic "topic1"
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 500)
    .load()
)

df = (
    df
    .select(F.from_json(F.col("value").cast("string"), input_schema).alias("data"))
    .select("data.*")
    .select(F.explode(ner_udf(F.col("body"))).alias("entity"))
    .select("entity.*")
    .groupBy("text")
    .agg(F.sum("count").alias("total_count"))
    .select(F.to_json(F.struct("text", "total_count")).alias("value"))
)

# df.show(truncate=False)

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
