package ru.beeline.dmp.school.de.lessonFinal.task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
   На основе класса AggregatesPostgres написать приложение, которое будет считать
   среднюю длительность подключения для каждого региона и сохранять результат в таблицу
   postgre формата
*/

object CallAggPostgresFinalTask extends App with Logging with Context {

  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)

  // spark сессия
  override val appName = "Lesson_4_CallAggPostgresFinalTask"

  val conf = new CallAggPostgresFinalTaskArg(args)

  import spark.implicits._

  val callDaily = spark
    .read
    .format("parquet")
    .load("/data/generated_parquet")
    .where($"event-date" === conf.eventDate.apply())

  callDaily.show(10)

  val avgCallRegionDf = callDaily
    .groupBy("event-date", "region")
    .agg(round(avg("call_length_sec"), 2).as("call_length_sec_avg"))


  avgCallRegionDf
    .write
    .mode("overwrite")
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", conf.urlPostgres.apply())
    .option("dbtable", conf.targetTable.apply())
    .option("user", conf.loginPostgres.apply())
    .option("password", conf.passwordPostgres.apply())
    .save()


  // проверка записанной иаблицы
  val avgCallRegionPostgres = spark
    .read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", conf.targetTable.apply())
    .option("url", conf.urlPostgres.apply())
    .option("user", conf.loginPostgres.apply())
    .option("password", conf.passwordPostgres.apply())
    .load()

  avgCallRegionPostgres.show()


}
