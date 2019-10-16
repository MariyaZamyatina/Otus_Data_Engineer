package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.renarde.wikiflow.consumer.DataDescription.expectedSchema

object AnalyticsConsumer extends App with LazyLogging {
  val appName: String = "analytics-consumer-example"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  logger.info("Initializing Analytics consumer")

  spark.sparkContext.setLogLevel("WARN")

  val inputStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wikiflow-topic")
    .option("startingOffsets", "earliest")
    .load()

  val transformedStream: DataFrame = inputStream
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    .filter($"value".isNotNull)
    .select(from_json($"value", expectedSchema).as("data")).select("data.*")
    .filter($"bot" =!= true)
    .withColumn("timestamp", $"timestamp".cast(TimestampType))
    .withWatermark("timestamp", "1 minute")
    .groupBy($"type", window($"timestamp", "5 minutes", "1 minute"))
    .count()
    .withColumn("timestamp", current_timestamp())
    .select($"type", $"count", $"timestamp")

  transformedStream.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/storage/analytics-consumer/checkpoints")
    .start("/storage/analytics-consumer/output")

  spark.streams.awaitAnyTermination()
}
