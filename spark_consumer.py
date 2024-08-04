from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, MapType, DoubleType, ArrayType
from pyspark.sql.functions import from_json, col, explode


spark = SparkSession.builder\
        .appName("SparkConsumer")\
        .config("spark.streaming.stopGracefullyOnShutdown", True)\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")\
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")\
        .config("spark.sql.shuffle.partitions", 4)\
        .master("local[*]")\
        .getOrCreate()
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/home/rihab/golden-system-411709-5116d50b308f.json")
spark.conf.set("spark.executor.heartbeatInterval", "20s")

kafka_topic= "velib_bikes"

schema = StructType([
    StructField("total_count", IntegerType(), True),
    StructField("results", ArrayType(StructType([
        StructField("stationcode", StringType(), True),
        StructField("name", StringType(), True),
        StructField("is_installed", StringType(), True),
        StructField("capacity", IntegerType(), True),
        StructField("numdocksavailable", IntegerType(), True),
        StructField("numbikesavailable", IntegerType(), True),
        StructField("mechanical", IntegerType(), True),
        StructField("ebike", IntegerType(), True),
        StructField("is_renting", StringType(), True),
        StructField("is_returning", StringType(), True),
        StructField("duedate", StringType(), True),
        StructField("coordonnees_geo", MapType(StringType(), DoubleType()), True),

        StructField("nom_arrondissement_communes", StringType(), True),
        StructField("code_insee_commune", StringType(), True),
        ])), True)
    ])

df_streaming = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "velib_bikes")\
        .option("startingOffsets", "earliest")\
        .option("failOnDataLoss", "false")\
        .load()

df = df_streaming.select(from_json(col('value').cast('string'), schema).alias("data"), col('timestamp')).select("data.*", "timestamp")

df_exploded = df.select("total_count", "timestamp", explode(col("results")).alias("result"))

df_result= df_exploded.select(
        col("total_count"),
        col("result.stationcode"),
        col("result.name"),
        col("result.is_installed"),
        col("result.capacity"),
        col("result.numdocksavailable"),
        col("result.numbikesavailable"),
        col("result.mechanical"),
        col("result.ebike"),
        col("result.is_renting"),
        col("result.is_returning"),
        col("result.duedate"),
        col("result.coordonnees_geo.lon").alias("logitude"), 
        col("result.coordonnees_geo.lat").alias("latitude"),
        col("result.nom_arrondissement_communes"),
        col("result.code_insee_commune"),
        col("timestamp"),
        )
window_duration = "1 minutes"
data_to_stream = df_result.withWatermark("timestamp", window_duration)
query = data_to_stream.writeStream\
        .trigger(processingTime=window_duration)\
        .option("checkpointLocation", "gs://bikes_data_rg_01/checkpoint")\
        .option("path", "gs://bikes_data_rg_01/bikes")\
        .outputMode("append")\
        .start()\
        .awaitTermination()
