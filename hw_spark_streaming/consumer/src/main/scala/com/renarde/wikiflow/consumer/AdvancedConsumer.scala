package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object AdvancedConsumer extends App with LazyLogging{

  val appName: String = "advanced-consumer-example"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  logger.info("Initializing Advanced consumer")

  val inputStream = spark.readStream
    .format("delta")
    .load("/storage/analytics-consumer/output")

  val consoleOutput = inputStream
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

  spark.streams.awaitAnyTermination()
}