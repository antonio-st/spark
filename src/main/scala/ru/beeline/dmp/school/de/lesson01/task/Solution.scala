package ru.beeline.dmp.school.de.lesson01.task

import org.apache.spark.sql.SparkSession

object Solution extends App {

	// подключаемся к Spark --------------------------------------------------------------------------------------------
	val spark: SparkSession = SparkSession
		.builder()
		.appName("School_DE_Lesson_01_Task")
		.enableHiveSupport()
		.getOrCreate()


	/** Метод печатает в консоли значение autoBroadcastJoinThreshold */
	def printConfigValue(spark: SparkSession): Unit = {

		println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

	}

	/** Метод изменяет значение autoBroadcastJoinThreshold */
	def changeConfigValue(spark: SparkSession, value: String): Unit = {

		spark.conf.set("spark.sql.autoBroadcastJoinThreshold", value)

	}

	printConfigValue(spark)

	changeConfigValue(spark, "20mb")

	printConfigValue(spark)

}
