package ru.beeline.dmp.practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.random.RandomRDDs._


/*
Класс генерирует rows_count записей формата csv в схеме "msisdn: Long, url: String".
Результат записывается в папку data.
Запускать через spark-submit на компьютере с установленным spark, либо в docker контейнере spark-master из
файла docker-compose.yaml командой
spark-submit --master spark://[container_id]:7077 --class ru.beeline.dmp.practice.CsvGenerator /jars/spark-scala-assembly-1.0.jar
 */
object CsvGenerator extends App with Logging {

  val rows_count: Long = 100


  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)


  // подключаемся к Spark --------------------------------------------------------------------------------------------
  val spark: SparkSession = SparkSession
    .builder()
    .appName("csv_generator")
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  /*
  функция-генератор строки по номеру msisdn. Для всех номеров, заканчивающихся на 0, возвращает "yandex.ru", для остальных -- "google.com"
   */

  def generateUrl(msisdn: Long): String =
    msisdn match {
      case msisdn: Long if msisdn % 10 == 0 => "yandex.ru"
      case _ => "google.com"
    }

  def generateUrlUdf = udf(generateUrl _)



  uniformRDD(sc, rows_count, 1)  // создание массива случайный чисел в интервале [0, 1]
    .map(x => (10000000 * x + 9990000000L).toLong)  // преобразование в номера вида 999*******
    .toDF("msisdn")
    .withColumn("url", generateUrlUdf(col("msisdn")))
    .write.mode("overwrite")
    .option("header", true)
    .format("csv")
    .save("/data/random_sample")


}
