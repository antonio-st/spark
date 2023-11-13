package ru.beeline.dmp.school.de.lesson03.demo

import org.apache.spark.internal.Logging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object DataFrameApiDemo extends App with Logging with Context {

  //  Logger.getRootLogger.setLevel(Level.WARN)
  //  val logger: Logger = Logger.getLogger(getClass.getName)
  //  logger.setLevel(Level.INFO)

  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)

  // spark сессия
  override val appName: String = "Lesson_3_DataFrameApiDemo"

  // фильтрация среди файлов в parquet
  val dfAllDays = spark.read.parquet("/data/events_daily")
  val dfOneDay = dfAllDays.where("date = '11-01-2020'")

  dfOneDay.show()

  import spark.implicits._

  // по данным выше выбираем 2 колонки, создаем третью разделяя текст по точке
  val categoriesSplit = {
    dfOneDay
      .select("category_id", "category_code").distinct()
      .withColumn("cat_array_split", split($"category_code", "\\."))
  }

  categoriesSplit.show(truncate = false)

  // разделяем df на 3 колонки, в каждой по 1 элементу из строки
  val categoriesLevels = {
    categoriesSplit
      .withColumn("cat_1", $"cat_array_split".getItem(0))
      .withColumn("cat_2", $"cat_array_split".getItem(1))
      .withColumn("cat_3", $"cat_array_split".getItem(2))
  }

  categoriesLevels.show(truncate = false)

  // сохранить на диск в формате parquet
  categoriesLevels
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .save("/data/categories")

  val categoriesLevelsFromDisk = {
    spark
      .read
      .parquet("/data/categories")
  }

  val dfOneDayWithLevels = {
    dfOneDay
      .join(categoriesLevelsFromDisk, Seq("category_id"), "left")
  }

  dfOneDayWithLevels
    .explain("extended")

  dfOneDayWithLevels.show()

}
