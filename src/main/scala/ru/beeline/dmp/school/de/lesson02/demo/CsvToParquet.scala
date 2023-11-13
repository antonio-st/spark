package ru.beeline.dmp.school.de.lesson02.demo

import org.apache.log4j.{Level, Logger}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.beeline.dmp.school.de.lesson02.data.Data._

object CsvToParquet extends App with Logging {
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)

  // подключаемся к Spark --------------------------------------------------------------------------------------------
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Lesson_02_Demo_csv_to_parquet")
    .enableHiveSupport()
    .getOrCreate()


  val dfOneDay = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/data/events_data_csv/2021-11-25")
  }

  dfOneDay.show


  // save to parquet
  {
    dfOneDay
      .write
      .mode("overwrite")
      .format("parquet")
      .save("/data/one_day")
  }

  // save partitioned
  {
    dfOneDay
      .write
      .mode("overwrite")
      .partitionBy("event_time")
      .format("parquet")
      .save("/data/events")
  }

  //save coalesce
  {
    dfOneDay.coalesce(1)
      .write
      .mode("overwrite")
      .format("parquet")
      .save("/data/coalesce_one")
  }

  // save csv to partitions
  val df_all = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/data/events.csv")
  }

  df_all.show(5)

  df_all
    .withColumn("date", date_format(col("event_time"), "MM-dd-yyyy"))
    .withColumn("event_time", date_format(col("event_time"), "MM-dd-yyyy"))
    .write
    .mode("overwrite")
    .partitionBy("event_time")
    .format("csv")
    .save("/data/events_2")

  df_all
    .withColumn("date", date_format(col("event_time"), "MM-dd-yyyy"))
    .withColumn("event_time", date_format(col("event_time"), "MM-dd-yyyy"))
    .write
    .mode("overwrite")
    .partitionBy("event_time")
    .format("parquet")
    .save("/data/events_daily")

}
