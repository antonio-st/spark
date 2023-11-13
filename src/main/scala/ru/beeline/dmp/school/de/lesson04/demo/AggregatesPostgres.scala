package ru.beeline.dmp.school.de.lesson04.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/*
spark-submit \
  --class ru.beeline.dmp.school.de.lesson04.demo.AggregatesPostgres \
  --master spark://1f0013c7d868:7077 \
  /jars/spark-scala-assembly-1.0.jar \
  --load-date 2021-11-25 \
  --source-path /data/events \
  --target-table events_brand_prices
*/

object AggregatesPostgres extends App with Logging {
  //  Logger.getRootLogger.setLevel(Level.WARN)
  //  val logger: Logger = Logger.getLogger(getClass.getName)
  //  logger.setLevel(Level.INFO)
  //  Logger.getLogger("org").setLevel(Level.ERROR)

  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)


  val conf = new AggregatesPostgresArgs(args)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Lesson_4_agg_to_postgres")
    .enableHiveSupport()
    .getOrCreate()

  val eventsDaily = spark
    .read
    .format("parquet")
    .load(conf.sourcePath.apply)
    .where(s"event_time='${conf.loadDate.apply}'")

  import spark.implicits._

  eventsDaily
    .groupBy("event_time", "brand")
    .agg(sum("price").as("sum_price"))
    .write
    .mode("overwrite")
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", conf.urlPostgres.apply)
    .option("dbtable", conf.targetTable.apply)
    .option("user", conf.loginPostgres.apply)
    .option("password", conf.passwordPostgres.apply)
    .save()

  // проверка записанной таблицы

  val eventsDailyDb = spark
    .read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", conf.urlPostgres.apply)
    .option("dbtable", conf.targetTable.apply)
    .option("user", conf.loginPostgres.apply)
    .option("password", conf.passwordPostgres.apply)
    .load()

  eventsDailyDb.show()

}
