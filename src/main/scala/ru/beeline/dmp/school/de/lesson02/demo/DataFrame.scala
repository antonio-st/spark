package ru.beeline.dmp.school.de.lesson02.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.beeline.dmp.school.de.lesson02.data.Data._

object DataFrame extends App with Logging {
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)

  // подключаемся к Spark --------------------------------------------------------------------------------------------
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Lesson_02_Demo_DataFrame")
    .enableHiveSupport()
    .getOrCreate()

  // DataFrame -------------------------------------------------------------------------------------------------------
  val dfData: DataFrame = spark
    .createDataFrame(subscriberHistory)
    .toDF("ctn", "report_date", "voice_minute", "sms_count")

  dfData.printSchema()
  dfData.show()

  val dfAvg = dfData
    .groupBy("ctn")
    .agg(
      avg("voice_minute").alias("avg_duration"),
      sum("voice_minute").alias("sum_duration")
    )

  dfAvg.show()


  // DataSet ---------------------------------------------------------------------------------------------------------
  //	import spark.implicits._
  //
  //	case class History(ctn: String, report_date: String, voice_minute: Double, sms_count: Double)
  //
  //
  //	val dsData = subscriberHistory
  //		.map(x => History(x._1, x._2, x._3, x._4))
  //		.toDS()
  //
  //	dsData.printSchema()
  //	dsData.show()

  // RDD[(String, String, Int, Int)] -> RDD[Row]
  val rddData = spark
    .sparkContext
    .parallelize(subscriberHistory)


  // Создаем DataFrame с помощью schema ------------------------------------------------------------------------------
  val schema = StructType(Array(
    StructField("ctn", StringType, nullable = false),
    StructField("report_date", StringType, nullable = false),
    StructField("voice_minute", DoubleType, nullable = false),
    StructField("sms_count", DoubleType, nullable = false)
  ))

  val dfDataWithSchema = spark
    .createDataFrame(
      rddData.map(x => Row(x._1, x._2, x._3.toDouble, x._4.toDouble)),
      schema
    )

  dfDataWithSchema.printSchema()
  dfDataWithSchema.show()


  // фильтрация и сортировка

  val dfFiltered = dfDataWithSchema
    .where("sms_count != 0")
    .where(dfDataWithSchema("sms_count") =!= 0)
    .filter(dfDataWithSchema("voice_minute").between(60, 600))
    .orderBy(dfDataWithSchema("sms_count").desc)

  dfFiltered.show()



  // Агрегирующие функции --------------------------------------------------------------------------------------------

  // кол-во уникальных номеров
  dfDataWithSchema.select("ctn", "sms_count").distinct().count()

  dfDataWithSchema.select(countDistinct("ctn")).show

  dfDataWithSchema.select(approx_count_distinct("ctn", 0.1)).show

  // агрегация по номерам и ветвление
  val dfAgg = dfDataWithSchema
    .groupBy("ctn")
    .agg(
      max(dfDataWithSchema("voice_minute")).as("max_voice_minute"),
      avg(dfDataWithSchema("voice_minute")).as("avg_voice_minute"),
      sum(dfDataWithSchema("voice_minute")).as("sum_voice_minute"),
      sum(dfDataWithSchema("voice_minute")).as("sum_sms_count"))
    .withColumn("sms_count_group",
      when(col("sum_sms_count") < 50, "0-49")
        .when(col("sum_sms_count") < 100, "50-99")
        .when(col("sum_sms_count") < 500, "100-499")
        .otherwise("more 500")
    )

  dfAgg.show()


}
