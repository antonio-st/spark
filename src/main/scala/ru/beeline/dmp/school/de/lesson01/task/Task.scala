package ru.beeline.dmp.school.de.lesson01.task

import org.apache.spark.sql.SparkSession

object Task extends App {

	// подключаемся к Spark --------------------------------------------------------------------------------------------
	val spark: SparkSession = SparkSession
		.builder()
		.appName("School_DE_Lesson_01_Task")
		.enableHiveSupport()
		.getOrCreate()


	/** Метод печатает в консоли значение autoBroadcastJoinThreshold */
	def printConfigValue(spark: SparkSession): Unit = {
		???
	}

	/** Метод изменяет значение autoBroadcastJoinThreshold */
	def changeConfigValue(spark: SparkSession, newValue: String): Unit = {
		???
	}


	printConfigValue(spark)
	changeConfigValue(spark, "100mb")
	printConfigValue(spark)

}
