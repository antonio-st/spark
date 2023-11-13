package ru.beeline.dmp.school.de.lesson02.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.beeline.dmp.school.de.lesson02.data.Data._

object Rdd extends App with Logging {
	Logger.getRootLogger.setLevel(Level.WARN)
	val logger: Logger = Logger.getLogger(getClass.getName)
	logger.setLevel(Level.INFO)
	Logger.getLogger("org").setLevel(Level.ERROR)

	// подключаемся к Spark --------------------------------------------------------------------------------------------
	val spark: SparkSession = SparkSession
		.builder()
		.appName("School_DE_Lesson_02_Demo")
		.enableHiveSupport()
		.getOrCreate()



	// RDD -------------------------------------------------------------------------------------------------------------
	println("RDD example")
	val rddData = spark
		.sparkContext
		.parallelize(subscriberHistory)

	println("rddData:")
	rddData.collect.foreach(println)

	val rddAvgDuration = rddData
		.map(x => (x._1, (x._3, 1)))
		.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
		.map(x => (x._1, x._2._1.toDouble / x._2._2.toDouble))

	println("rddAges:")
	rddAvgDuration.collect.foreach(println)

	println("rddAges by steps:")
	println("stepOne = rddData.map(x => (x._1, (x._3, 1)))")
	val stepOne = rddData.map(x => (x._1, (x._3, 1)))

	stepOne.collect.foreach(println)


	println("rddAges by steps:")
	println("stepTwo = stepOne.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))")
	val stepTwo = stepOne.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

	stepTwo.collect.foreach(println)


}
