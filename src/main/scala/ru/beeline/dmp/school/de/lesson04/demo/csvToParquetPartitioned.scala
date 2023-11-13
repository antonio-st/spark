package ru.beeline.dmp.school.de.lesson04.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

/*
spark-submit \
  --class ru.beeline.dmp.school.de.lesson04.demo.csvToParquetPartitioned \
  --master spark://1f0013c7d868:7077 \
  /jars/spark-scala-assembly-1.0.jar \
  --load-date 2021-11-25 \
  --source-path /data/events_data_csv \
  --target-path /data/events
*/

object csvToParquetPartitioned extends App with Logging {

  //  Logger.getRootLogger.setLevel(Level.WARN)
  //  val logger: Logger = Logger.getLogger(getClass.getName)
  //  logger.setLevel(Level.INFO)
  //  Logger.getLogger("org").setLevel(Level.ERROR)

  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  val conf = new csvToParquetPartitionedArgs(args)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Lesson_4_csvToParquetPartitionedArgs")
    .enableHiveSupport()
    .getOrCreate()

  val eventsSchema = StructType(
    Array(
      StructField("event_time", DateType, true),
      StructField("event_type", StringType, true),
      StructField("product_id", IntegerType, true),
      StructField("category_id", LongType, true),
      StructField("category_code", StringType, true),
      StructField("brand", StringType, true),
      StructField("price", FloatType, true),
      StructField("user_id", IntegerType, true),
      StructField("user_session", StringType, true),
      StructField("date", DateType, true)
    )
  )

  val dfOneDay = spark
    .read
    .schema(eventsSchema)
    .option("header", "true")
    .csv(s"${conf.sourcePath.apply}/${conf.loadDate.apply}")

  dfOneDay.show(5)

  dfOneDay
    .write
    .partitionBy("event_time")
    .mode("overwrite")
    .format("parquet")
    .save(conf.targetPath.apply)

}
