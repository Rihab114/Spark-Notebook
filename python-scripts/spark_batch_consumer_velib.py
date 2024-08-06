from pyspark.sql import SparkSession
import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
import tempfile
import json

# Create the Spark Session
from pyspark.sql import SparkSession

gcs_connector_path='/Users/bassem.mathlouthi/Desktop/Big_Data_Training/Spark-Notebook/gcs-connector-hadoop3-latest.jar'
kafka_jar_path='spark-sql-kafka-0-10_2.12-3.5.0.jar'

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .config("spark.jars", gcs_connector_path) \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "../terraform/my-creds.json") \
    .config("spark.sql.shuffle.partitions", "1") \
    .master("local[*]") \
    .getOrCreate()



topic="velib"
# Create the streaming_df to read from kafka
streaming_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()


final_df=streaming_df.select(col('value').cast('string').alias('Velib_raw_json'),col('offset'),col('timestamp'))


#using with column and get_json_object methods to create the structure needed from the raw json response
final_df_0=final_df.withColumn('total_count',(get_json_object(col('Velib_raw_json'),'$.total_count')).cast(IntegerType()))\
                        .withColumn('stationcode',(get_json_object(col('Velib_raw_json'),'$.results[0].stationcode')).cast(IntegerType()))\
                        .withColumn('name',(get_json_object(col('Velib_raw_json'),'$.results[0].name')))\
                        .withColumn('is_installed',(get_json_object(col('Velib_raw_json'),'$.results[0].is_installed')))\
                        .withColumn('capacity',(get_json_object(col('Velib_raw_json'),'$.results[0].capacity')).cast(IntegerType()))\
                        .withColumn('numbikesavailable',(get_json_object(col('Velib_raw_json'),'$.results[0].numbikesavailable')).cast(IntegerType()))\
                        .withColumn('numbikesavailable',(get_json_object(col('Velib_raw_json'),'$.results[0].numbikesavailable')).cast(IntegerType()))\
                        .withColumn('mechanical',(get_json_object(col('Velib_raw_json'),'$.results[0].mechanical')).cast(IntegerType()))\
                        .withColumn('ebike',(get_json_object(col('Velib_raw_json'),'$.results[0].ebike')).cast(IntegerType()))\
                        .withColumn('is_renting',(get_json_object(col('Velib_raw_json'),'$.results[0].is_renting')))\
                        .withColumn('is_returning',(get_json_object(col('Velib_raw_json'),'$.results[0].is_returning')))\
                        .withColumn('duedate',(get_json_object(col('Velib_raw_json'),'$.results[0].duedate')).cast(TimestampType()))\
                        .withColumn('coordonnees_geo_lon',(get_json_object(col('Velib_raw_json'),'$.results[0].coordonnees_geo.lon')).cast(FloatType()))\
                        .withColumn('coordonnees_geo_lat',(get_json_object(col('Velib_raw_json'),'$.results[0].coordonnees_geo.lat')).cast(FloatType()))\
                        .withColumn('nom_arrondissement_communes',(get_json_object(col('Velib_raw_json'),'$.results[0].nom_arrondissement_communes')))\
                        .withColumn('code_insee_commune',(get_json_object(col('Velib_raw_json'),'$.results[0].code_insee_commune')))
final_df_0.writeStream.format("console").start()

for i in range(1,100):
    dynamic_station_path=f'$.results[{i}].stationcode'
    dynamic_name_path=f'$.results[{i}].name'
    dynamic_is_installed_path=f'$.results[{i}].is_installed'
    dynamic_capacity_path=f'$.results[{i}].capacity'
    dynamic_numdocksavailable_path=f'$.results[{i}].numdocksavailable'
    dynamic_numbikesavailable_path=f'$.results[{i}].numbikesavailable'
    dynamic_ebike_path=f'$.results[{i}].ebike'
    dynamic_is_renting_path=f'$.results[{i}].is_renting'
    dynamic_is_returning_path=f'$.results[{i}].is_returning'
    dynamic_duedate_path=f'$.results[{i}].duedate'
    dynamic_coordonnees_geo_lon_path=f'$.results[{i}].coordonnees_geo.lon'
    dynamic_coordonnees_geo_lat_path=f'$.results[{i}].coordonnees_geo.lat'
    dynamic_nom_arrondissement_communes_path=f'$.results[{i}].nom_arrondissement_communes'
    dynamic_code_insee_commune_communes_path=f'$.results[{i}].code_insee_commune'
    


    final_df_0=final_df_0.union(final_df.withColumn('total_count',(get_json_object(col('Velib_raw_json'),'$.total_count')))\
                            .withColumn('stationcode',(get_json_object(col('Velib_raw_json'),dynamic_station_path)))\
                            .withColumn('name',(get_json_object(col('Velib_raw_json'),dynamic_name_path)))\
                            .withColumn('is_installed',(get_json_object(col('Velib_raw_json'),dynamic_is_installed_path)))\
                            .withColumn('capacity',(get_json_object(col('Velib_raw_json'),dynamic_capacity_path)))\
                            .withColumn('numdocksavailable',(get_json_object(col('Velib_raw_json'),dynamic_numdocksavailable_path)))\
                            .withColumn('numbikesavailable',(get_json_object(col('Velib_raw_json'),dynamic_numbikesavailable_path)))\
                            .withColumn('ebike',(get_json_object(col('Velib_raw_json'),dynamic_ebike_path)))\
                            .withColumn('is_renting',(get_json_object(col('Velib_raw_json'),dynamic_is_renting_path)))\
                            .withColumn('is_returning',(get_json_object(col('Velib_raw_json'),dynamic_is_returning_path)))\
                            .withColumn('duedate',(get_json_object(col('Velib_raw_json'),dynamic_duedate_path)))\
                            .withColumn('coordonnees_geo_lon',(get_json_object(col('Velib_raw_json'),dynamic_coordonnees_geo_lon_path)))\
                            .withColumn('coordonnees_geo_lat',(get_json_object(col('Velib_raw_json'),dynamic_coordonnees_geo_lat_path)))\
                            .withColumn('nom_arrondissement_communes',(get_json_object(col('Velib_raw_json'),dynamic_nom_arrondissement_communes_path)))\
                            .withColumn('code_insee_commune',(get_json_object(col('Velib_raw_json'),dynamic_code_insee_commune_communes_path))))

#REPARTITION(1)
#write raw kafka message to gcs
# using repartition(1) will ensure to have one single parquet file written to gcs per batch

#==> with this config we will store a single parquet file for every batch (every window 3min)

my_bucketname='bassem-zoomcamp-2024'
window_duration = '3 minutes'
destination_folder='kafka_distination'
query = final_df_0.repartition(1) \
  .writeStream\
  .trigger(processingTime=window_duration)\
  .option("checkpointLocation", f"gs://{my_bucketname}/{destination_folder}/checkpoint/")\
  .option("path",f"gs://{my_bucketname}/{destination_folder}")\
  .outputMode("append")\
  .start()\
  .awaitTermination()