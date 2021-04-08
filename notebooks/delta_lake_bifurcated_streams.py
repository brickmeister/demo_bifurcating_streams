# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Introduction
# MAGIC 
# MAGIC This is a demo of data engineering pipelines involving streaming gzipped data on the Databricks platform. This notebook demonstrates Spark DataFrames based unzipping of gzip files as well as RDD based methods. Data is then ingested into Delta Lake where it is further downstream processed via filters for different schemas. This demonstration of bifurcating streams shows how a single ingestion stream can be written to multiple tables.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Architecture
# MAGIC <img src='https://raw.githubusercontent.com/brickmeister/demo_bifurcating_streams/main/img/Bifurcating%20Streams.png' /img>

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Gzipped CSV files in DBFS

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Read in Databricks Dataset

# COMMAND ----------

# DBTITLE 1,Ingest clickstream Data
# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Setup a data set to create gzipped json files
# MAGIC */
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.rand;
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC 
# MAGIC // Setup a file schema for loading json files
# MAGIC val file_schema : StructType = StructType(
# MAGIC                                 List(StructField("json", StringType, true)));
# MAGIC 
# MAGIC // setup the starting offset
# MAGIC val df_clickstream : DataFrame = spark.read
# MAGIC                                       .format("csv")
# MAGIC                                       .option("delimiter", "|")
# MAGIC                                       .option("header", "false")
# MAGIC                                       .schema(file_schema)
# MAGIC                                       .load("dbfs:/databricks-datasets/structured-streaming/events/*.json")
# MAGIC                                       .withColumn("schema_type", (rand(3) * 10) .cast("int") * 10 % 3)
# MAGIC 
# MAGIC // display the dataframe
# MAGIC display(df_clickstream)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Gzipped CSV FIles

# COMMAND ----------

# DBTITLE 1,Create Gzipped CSV Files
# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Write out files to gzipped format
# MAGIC */
# MAGIC 
# MAGIC df_clickstream.write
# MAGIC               .format("csv")
# MAGIC               .option("compression", "gzip")
# MAGIC               .option("header", "true")
# MAGIC               .option("checkpointLocation", "dbfs:/tmp/gzipped_clickstream_checkpoint")
# MAGIC               .save("dbfs:/tmp/gzipped_clickstream/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Spark Structured Streaming

# COMMAND ----------

# DBTITLE 1,List gzipped files
# MAGIC %fs
# MAGIC 
# MAGIC ls dbfs:/tmp/gzipped_clickstream

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Ingest Data from Gzipped CSV

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Dataframe GZIP Reader

# COMMAND ----------

# DBTITLE 1,Unzip Files using DataFrames
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC import org.apache.spark.sql.streaming.Trigger;
# MAGIC 
# MAGIC /*
# MAGIC   Read in gzipped files
# MAGIC */
# MAGIC 
# MAGIC val file_schema = StructType(
# MAGIC                     List(StructField("json", StringType),
# MAGIC                          StructField("schema_type", IntegerType))); 
# MAGIC 
# MAGIC val df : DataFrame = spark.readStream
# MAGIC                           .format("csv")
# MAGIC                           .option("header", true)
# MAGIC                           .schema(file_schema)
# MAGIC                           .load("dbfs:/tmp/gzipped_clickstream/*.gz")

# COMMAND ----------

# DBTITLE 1,Write To Delta Lake
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.streaming.Trigger;
# MAGIC 
# MAGIC /*
# MAGIC   Write streaming data into delta lake
# MAGIC */
# MAGIC 
# MAGIC df.writeStream
# MAGIC   .format("delta")
# MAGIC   .trigger(Trigger.Once)
# MAGIC   .option("checkpointLocation", "dbfs:/tmp/demo_data_checkpoint/")
# MAGIC   .start("dbfs:/tmp/demo_streaming_data");

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### RDD GZIP Reader

# COMMAND ----------

# DBTITLE 1,Unzip Files Using RDD
# MAGIC %python
# MAGIC 
# MAGIC from typing import List
# MAGIC import zlib
# MAGIC from glob import glob
# MAGIC import csv
# MAGIC from pyspark import RDD
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.types import StructField, StructType, IntegerType, StringType
# MAGIC 
# MAGIC """
# MAGIC   Unzip files via an RDD
# MAGIC """
# MAGIC 
# MAGIC # Get a list of all files to unzip
# MAGIC gzipped_files : List[str] = [a.replace("/dbfs/", "dbfs:/")\
# MAGIC                                for a in glob("/dbfs/tmp/gzipped_clickstream/*.gz")]
# MAGIC   
# MAGIC file_schema : StructType = StructType(
# MAGIC                               [StructField("json", StringType()),
# MAGIC                                StructField("schema_type", IntegerType())]) 
# MAGIC   
# MAGIC # create a rdd for unzipping gzipped files
# MAGIC gzipped_rdd : RDD = spark.read\
# MAGIC                          .format("binaryFile")\
# MAGIC                          .load(gzipped_files)\
# MAGIC                          .rdd\
# MAGIC                          .map(lambda _: (_[0], 
# MAGIC                                          (zlib.decompress(_[3], 15+32))\
# MAGIC                                               .decode("utf-8")))\
# MAGIC                          .flatMap(lambda _: csv.reader(_[1].splitlines(),
# MAGIC                                                        escapechar='\\',
# MAGIC                                                        skipinitialspace = True))\
# MAGIC                          .filter(lambda _: _ != ['json', 'schema_type'])\
# MAGIC                          .map(lambda _: (str(_[0]), int(_[1])))
# MAGIC 
# MAGIC # Create a spark Dataframe from RDD
# MAGIC # DO NOT CALL COLLECT
# MAGIC df : DataFrame = spark.createDataFrame(gzipped_rdd, schema = file_schema)

# COMMAND ----------

# DBTITLE 1,Write to Delta Lake
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC   Write dataframe to delta lake
# MAGIC """
# MAGIC 
# MAGIC df.write\
# MAGIC   .format("delta")\
# MAGIC   .mode("overwrite")\
# MAGIC   .save("dbfs:/tmp/demo_streaming_data");

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup Delta Stream Source

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Read from delta
# MAGIC */
# MAGIC 
# MAGIC val delta_stream = spark.readStream
# MAGIC                         .format("delta")
# MAGIC                         .option("startingVerson", "latest")
# MAGIC                         .load("dbfs:/tmp/demo_streaming_data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write Bifurcated Streams

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Type 1 Stream

# COMMAND ----------

# DBTITLE 1,Schema Type 1 Write Stream
# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Decode data for dataframe with schema type 1
# MAGIC */
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.from_json;
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC import org.apache.spark.sql.streaming.Trigger;
# MAGIC import org.apache.spark.sql.streaming.StreamingQuery;
# MAGIC 
# MAGIC val schema_type_1 : StructType = StructType(
# MAGIC                                     List(StructField("time", IntegerType),
# MAGIC                                          StructField("action", StringType)));
# MAGIC 
# MAGIC val df_type_1 : StreamingQuery = delta_stream.where($"schema_type" === 0)
# MAGIC                                         .select(from_json($"json", 
# MAGIC                                                           schema_type_1 ).alias("json"))
# MAGIC                                         .select($"json.*")
# MAGIC                                         .writeStream
# MAGIC                                         .format("delta")
# MAGIC                                         .trigger(Trigger.Once)
# MAGIC                                         .option("checkpointLocation", "dbfs:/tmp/demo_streaming_data_1_checkpoint")
# MAGIC                                         .start("dbfs:/tmp/demo_streaming_data_1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Type 2 Stream

# COMMAND ----------

# DBTITLE 1,Schema Type 2 Write Stream
# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Decode data for dataframe with schema type 2
# MAGIC */
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.from_json;
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC import org.apache.spark.sql.streaming.Trigger;
# MAGIC import org.apache.spark.sql.streaming.StreamingQuery;
# MAGIC 
# MAGIC val schema_type_2 : StructType = StructType(
# MAGIC                                     List(StructField("time", IntegerType),
# MAGIC                                          StructField("action", StringType)));
# MAGIC 
# MAGIC val df_type_2 : StreamingQuery = delta_stream.where($"schema_type" === 0)
# MAGIC                                         .select(from_json($"json", 
# MAGIC                                                           schema_type_2 ).alias("json"))
# MAGIC                                         .select($"json.*")
# MAGIC                                         .writeStream
# MAGIC                                         .format("delta")
# MAGIC                                         .trigger(Trigger.Once)
# MAGIC                                         .option("checkpointLocation", "dbfs:/tmp/demo_streaming_data_2_checkpoint")
# MAGIC                                         .start("dbfs:/tmp/demo_streaming_data_2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Type 3 Stream

# COMMAND ----------

# DBTITLE 1,Schema Type 3 Stream
# MAGIC %scala
# MAGIC 
# MAGIC /*
# MAGIC   Decode data for dataframe with schema type 1
# MAGIC */
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.from_json;
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
# MAGIC import org.apache.spark.sql.DataFrame;
# MAGIC import org.apache.spark.sql.streaming.Trigger;
# MAGIC import org.apache.spark.sql.streaming.StreamingQuery;
# MAGIC 
# MAGIC val schema_type_3 : StructType = StructType(
# MAGIC                                     List(StructField("time", IntegerType),
# MAGIC                                          StructField("action", StringType)));
# MAGIC 
# MAGIC val df_type_3 : StreamingQuery = delta_stream.where($"schema_type" === 0)
# MAGIC                                         .select(from_json($"json", 
# MAGIC                                                           schema_type_3 ).alias("json"))
# MAGIC                                         .select($"json.*")
# MAGIC                                         .writeStream
# MAGIC                                         .format("delta")
# MAGIC                                         .trigger(Trigger.Once)
# MAGIC                                         .option("checkpointLocation", "dbfs:/tmp/demo_streaming_data_3_checkpoint")
# MAGIC                                         .start("dbfs:/tmp/demo_streaming_data_3")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Cleanup

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from typing import List
# MAGIC 
# MAGIC """
# MAGIC Clean up all files written
# MAGIC """
# MAGIC 
# MAGIC _paths : List[str] = ["dbfs:/tmp/demo_streaming_data_1", 
# MAGIC                       "dbfs:/tmp/demo_streaming_data_2",
# MAGIC                       "dbfs:/tmp/demo_streaming_data_3",
# MAGIC                       "dbfs:/tmp/demo_streaming_data",
# MAGIC                       "dbfs:/tmp/demo_streaming_data_1_checkpoint",
# MAGIC                       "dbfs:/tmp/demo_streaming_data_2_checkpoint",
# MAGIC                       "dbfs:/tmp/demo_streaming_data_3_checkpoint",
# MAGIC                       "dbfs:/tmp/gzipped_clickstream_checkpoint",
# MAGIC                       "dbfs:/tmp/gzipped_clickstream",
# MAGIC                       "dbfs:/tmp/demo_data_checkpoint"]
# MAGIC   
# MAGIC for a in _paths:
# MAGIC     dbutils.fs.rm(a, recurse = True)
