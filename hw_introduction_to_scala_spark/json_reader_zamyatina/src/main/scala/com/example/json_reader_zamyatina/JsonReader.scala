package com.example.json_reader_zamyatina
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class Wine (id: Option[Int],
                 country: Option[String],
                 points: Option[Int],
                 price: Option[Int],
                 title: Option[String],
                 variety: Option[String],
                 winery: Option[String])

object JsonReader extends App{
  val inputFile = args(0)

  val sparkSession = SparkSession
    .builder()
    .appName("JsonReaderApp")
    .master("local[*]")
    .getOrCreate()

  Runner.run(sparkSession, inputFile)
}

object Runner {
  implicit val formats: Formats = DefaultFormats

  def run(session: SparkSession, inputFile: String): Unit = {
    val sc = session.sparkContext
    val rdd = sc.textFile(inputFile)
    rdd.map(line => parse(line).extract[Wine]).foreach(println)
  }
}