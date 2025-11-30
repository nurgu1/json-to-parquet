import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, explode


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_PATH = "s3://aviation-flights-raw-mglgx7/year=*/month=*/day=*/hour=*/flights.json"
TARGET_PATH = "s3://aviation-flights-converted-mglgx7/output/"

print("RAW_PATH =", RAW_PATH)
print("TARGET_PATH =", TARGET_PATH)

df_raw = spark.read.format("json").load(RAW_PATH)
df_raw.printSchema()


df_exploded = df_raw.select(explode(col("data")).alias("data"))


df_flat = df_exploded.select(
    col("data.flight_date").alias("flight_date"),
    col("data.flight_status").alias("flight_status"),

    col("data.airline.name").alias("airline_name"),
    col("data.airline.iata").alias("airline_iata"),
    col("data.airline.icao").alias("airline_icao"),

    col("data.flight.number").alias("flight_number"),
    col("data.flight.iata").alias("flight_iata"),
    col("data.flight.icao").alias("flight_icao"),

    col("data.departure.airport").alias("departure_airport"),
    col("data.departure.iata").alias("departure_iata"),
    col("data.departure.scheduled").alias("departure_scheduled"),
    col("data.departure.actual").alias("departure_actual"),

    col("data.arrival.airport").alias("arrival_airport"),
    col("data.arrival.iata").alias("arrival_iata"),
    col("data.arrival.scheduled").alias("arrival_scheduled"),
    col("data.arrival.actual").alias("arrival_actual"),

    col("data.aircraft").alias("aircraft_raw"),
    col("data.live").alias("live_raw")
)

print("COUNT:", df_flat.count())

df_flat.write.mode("overwrite").parquet(TARGET_PATH)

job.commit()
print("DONE")
