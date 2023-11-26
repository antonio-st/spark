package ru.beeline.dmp.school.de.lesson01.task

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import scala.util.Try


object Task extends App with Logging {

  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)

  // подключаемся к Spark --------------------------------------------------------------------------------------------
  val spark: SparkSession = SparkSession
    .builder()
    .appName("School_DE_Lesson_01_Task")
    .enableHiveSupport()
    .getOrCreate()


  /** Метод печатает в консоли значение autoBroadcastJoinThreshold */
  def printConfigValue(spark: SparkSession): Unit = {
    val valueConf =
      ((spark.conf.get("spark.sql.autoBroadcastJoinThreshold").split("b")(0).toInt) / 1048576)

    println(s"${valueConf} Mb")

  }

  /** Метод печатает в консоли значение Shuffle Partitions */
  def printConfigShufflePartitions(spark: SparkSession): Unit = {
    val valueConf =
      spark.conf.get("spark.sql.shuffle.partitions")
    println(valueConf)
  }


  /** Метод изменяет значение autoBroadcastJoinThreshold */
  def changeConfigValue(spark: SparkSession, newValue: String): Unit = {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", newValue)
  }

  /** Метод изменяет значение Shuffle Partitions */
  def changeConfigShufflePartitions(spark: SparkSession, value: Int): Unit = {
    val valueConf =
      spark.conf.set("spark.sql.shuffle.partitions", value)
  }

  printConfigValue(spark)
  changeConfigValue(spark, "104857600b")
  printConfigValue(spark)


  printConfigShufflePartitions(spark)
  changeConfigShufflePartitions(spark, 10)
  printConfigShufflePartitions(spark)
}
