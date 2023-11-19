package ru.beeline.dmp.school.de.lessonFinal.task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.random.RandomRDDs._

/*
spark-submit \
  --class ru.beeline.dmp.school.de.lessonFinal.task.CsvGeneratorFinalTask \
  --master spark://1f0013c7d868:7077 \
  /jars/spark-scala-assembly-1.0.jar \
  --event-date 2023-10-01 \
  --target-path /data/generated_csv \
  --sample-count 10000
*/
// task 1
object CsvGeneratorFinalTask extends App with Logging with Context {


  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)

  // spark сессия
  override val appName: String = "Lesson_4_CsvGeneratorFinalTask"
  val sc = spark.sparkContext

  import spark.implicits._

  // аргументы
  val conf = new CsvGeneratorFinalTaskArg(args)

  /*
  функция-генератор строки по номеру msisdn генерируем маркеты регионов
   */

  def generateUrl(msisdn: Long): String =
    msisdn match {
      case msisdn: Long if msisdn % 3 == 0 => "MSK"
      case msisdn: Long if msisdn % 4 == 0 => "SPB"
      case _ => "ccc"
    }

  def generateUrlUdf = udf(generateUrl _)

  val dfGenerate = uniformRDD(sc, conf.sampleCount.apply(), 1) // создание массива случайный чисел в интервале [0, 1]
    .map(x => (10000000 * x + 9990000000L).toLong) // преобразование в номера вида 999*******
    .toDF("msisdn")
    .withColumn("region", generateUrlUdf(col("msisdn")))
    .withColumn("event-date", lit(conf.eventDate.apply()))
    .withColumn("call_length_sec", round(rand() * (600 - 5) + 5, 0).cast("Int"))

  dfGenerate.printSchema()

  dfGenerate.write.mode("overwrite")
    .option("header", "true")
    .partitionBy("event-date")
    .format("csv")
    .save(conf.targetPath.apply())

  val checkCallCount = dfGenerate
    .groupBy("region")
    .agg(count("msisdn").as("count_call"))
    .withColumn("perc", ($"count_call" / conf.sampleCount.apply()) * 100)

  checkCallCount.show()

  //to_date(col("Input"), "MM/dd/yyyy").as("to_date")
  //.withColumn("call_length_sec", lit(current_date().cast("date")))
  //.withColumn("call_length_sec", second(current_timestamp()))
  //.withColumn("isVal", when(rand() > 0.5, 1).otherwise(0))
  //  val randomGenerator = new scala.util.Random
  //  Seq.fill(100)(randomGenerator.nextInt)
}
