package com.example.boston_crimes_map_zamyatina

import com.example.boston_crimes_map_zamyatina.BostonCrimesMap.sparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import scala.collection.mutable

object BostonCrimesMap extends App {
  if (args.length != 3) {
    println("Missing some arguments")
    sys.exit(-1)
  }

  val dataFiles = (args(0), args(1))
  val outputFolder = args(2)

  val sparkSession = SparkSession
    .builder()
    .appName("BostonCrimesMapApp")
    .master("local[*]")
    .getOrCreate()

  val data = BostonCrimeMapStats.prepareData(sparkSession, dataFiles)
  val crimesTotal = BostonCrimeMapStats.getCrimesTotal(data)
  val crimesMonthly = BostonCrimeMapStats.getCrimesMonthly(sparkSession, data)
  val frequentCrimeTypes = BostonCrimeMapStats.getFrequentCrimeTypes(data)
  val coordinates = BostonCrimeMapStats.getCoordinates(data)

  val result = crimesTotal
    .join(crimesMonthly, Seq("DISTRICT"))
    .join(frequentCrimeTypes, Seq("DISTRICT"))
    .join(coordinates, Seq("DISTRICT"))

  result.repartition(1).write.parquet(outputFolder)
}

object BostonCrimeMapStats {
  import sparkSession.implicits._

  def prepareData(session: SparkSession, dataFiles: (String, String)): DataFrame = {
    val (crimesFile, offenseCodesFile) = dataFiles

    val crimes = session
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(crimesFile)

    val offense_codes = session
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(offenseCodesFile)
      .withColumn("CRIME_TYPE", trim(split($"NAME", "-")(0)))
      .drop("NAME")

    val offense_codes_bd = broadcast(offense_codes)

    val data = crimes
      .select("INCIDENT_NUMBER", "OFFENSE_CODE", "DISTRICT", "MONTH", "Lat", "Long")
      .filter($"DISTRICT".isNotNull)
      .join(offense_codes_bd, $"CODE" === $"OFFENSE_CODE")
      .drop("CODE")
      .cache()

    data
  }

  def getCrimesTotal(data: DataFrame): DataFrame = {
    data
      .groupBy($"DISTRICT")
      .agg(count($"INCIDENT_NUMBER") as "CRIMES_TOTAL")
  }

  def getCrimesMonthly(session: SparkSession, data: DataFrame): DataFrame = {
    data.createOrReplaceTempView("data_view")

    session.sql(
      """
        SELECT DISTRICT, percentile_approx(crimes_total, 0.5) as CRIMES_MONTHLY
        FROM (SELECT DISTRICT, COUNT(INCIDENT_NUMBER) AS crimes_total FROM data_view GROUP BY DISTRICT, MONTH)
        GROUP BY DISTRICT
      """)
  }

  def getFrequentCrimeTypes(data: DataFrame): DataFrame = {
    def mkStringUDF: UserDefinedFunction = udf((array: mutable.WrappedArray[String]) => array.toList.mkString(", "))

    val window = Window.partitionBy($"DISTRICT").orderBy($"CRIMES_TOTAL".desc)

    data
      .groupBy($"DISTRICT", $"CRIME_TYPE")
      .agg(count($"INCIDENT_NUMBER") as "CRIMES_TOTAL")
      .withColumn("rank", row_number().over(window))
      .filter($"rank" <= 3)
      .groupBy($"DISTRICT").agg(collect_list($"CRIME_TYPE") as "crime_type_list")
      .withColumn("FREQUENT_CRIME_TYPES", mkStringUDF($"crime_type_list"))
      // spark 2.4.0
      //.withColumn("FREQUENT_CRIME_TYPES", array_join($"crime_type_list", ", "))
      .drop("crime_type_list")
  }

  def getCoordinates(data: DataFrame): DataFrame = {
    data
      .groupBy($"DISTRICT")
      .agg(mean($"lat") as "LAT", mean($"Long") as "LNG")
  }
}
