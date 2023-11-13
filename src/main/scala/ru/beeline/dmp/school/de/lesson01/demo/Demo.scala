package ru.beeline.dmp.school.de.lesson01.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Demo extends App  with Logging {

	Logger.getRootLogger.setLevel(Level.WARN)
	val logger: Logger = Logger.getLogger(getClass.getName)
	logger.setLevel(Level.INFO)

	// подключаемся к Spark --------------------------------------------------------------------------------------------
	val spark = SparkSession
		.builder()
		.appName("School_DE_Lesson_01_Demo")
		.enableHiveSupport()
		.getOrCreate()

	// печатаем config -------------------------------------------------------------------------------------------------
	spark.conf.getAll.foreach(c => println(c)) // полная конструкция

	spark.conf.getAll.foreach(println) // короткая запись

	spark.conf.getAll.foreach(c => println(s"Key: ${c._1} | Value: ${c._2}")) // делаем красивую строку

	import spark.implicits._

	val df = Seq(
		(1, "Moscow", 15),
		(2, "Pekin", 21),
		(3, "Rio", 3),
		(4, "Burkina-Faso", 1),
		(5, "Stambul", 8)
	).toDF("id", "city", "populations")

	df.show()
	df
		.write
		.option("header", "true")
		.mode("overwrite")
		.format("csv")
		.save("/data/city")
//		.write
//		.mode("overwrite")
//		.format("parquet")
//		.save("/data/one_day")
}
