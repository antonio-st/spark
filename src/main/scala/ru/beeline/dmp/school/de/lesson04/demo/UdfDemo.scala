package ru.beeline.dmp.school.de.lesson04.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

/*
spark-submit --master spark://8f8c13c6eaf1:7077 \
--class ru.beeline.dmp.school.de.lesson04.demo.UdfDemo \
/jars/spark-scala-assembly-1.0.jar
 */
object UdfDemo  extends App with Logging {
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("lesson04_udf")
    .enableHiveSupport()
    .getOrCreate()

  val events = spark.read.parquet("/data/events")
  events.printSchema
  events.show(false)

  val getDayNumber = (date: String) => {
    date.split("-")(2)
  }
  val getDayNumberUDF = udf(getDayNumber)

  val eventsWithDay = events
    .withColumn("day_of_month", getDayNumberUDF(col("event_time")))

  eventsWithDay.show(false)

  eventsWithDay.where("day_of_month='01'").explain
  events.where("event_time = '2019-11-01'").explain

}
