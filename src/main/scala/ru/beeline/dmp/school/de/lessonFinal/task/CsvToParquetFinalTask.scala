package ru.beeline.dmp.school.de.lessonFinal.task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

/*
spark-submit \
  --class ru.beeline.dmp.school.de.lessonFinal.task.CsvToParquetFinalTask \
  --master spark://1f0013c7d868:7077 \
  /jars/spark-scala-assembly-1.0.jar \
  --event-date 2023-10-01 \
  --target-path /data/generated_parquet
*/

/* task 2
 На основе класса CsvToParquetPartitioned написать приложение,
 которое преобразует данные формата csv в формат parquet.
*/

object CsvToParquetFinalTask extends App with Logging with Context {

  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)

  // spark сессия
  val appName = "Lesson_4_CsvToParquetFinalTask"

  // аргументы
  val conf = new CsvToParquetFinalTaskArg(args)

  val callSchema = StructType(
    Array(
      StructField("msisdn", LongType, true),
      StructField("region", StringType, true),
      StructField("call_length_sec", IntegerType, true)
    )
  )

  val dfGenerateCsv = spark
    .read
    .schema(callSchema)
    .option("header", "true")
    .csv("/data/generated_csv")


  dfGenerateCsv
    .write
    .partitionBy("event-date")
    .mode("overwrite")
    .format("parquet")
    .save(conf.targetPath.apply())


}
