package ru.beeline.dmp.school.de.lesson03.task

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Solution extends App {

	// сделать join vs broadcast

	// функция для подключения к спарку
	def sparkConnect(login: String): SparkSession = {

		SparkSession
			.builder()
			.appName(s"School_DE_Lesson_03_Task_$login")
			.getOrCreate()

	}

	// функция для чтения csv
	def readCSV(spark: SparkSession, schema: StructType, path: String): DataFrame ={

		spark
			.read
			.format("csv")
			.option("delimiter", ",")
			.option("header", "true")
			.option("dateFormat", "dd-MM-yyyy")
			.schema(schema)
			.load(path)

	}

	def joinDFs(dfCsv1: DataFrame, dfCsv2: DataFrame): DataFrame ={

		dfCsv1
			.join(dfCsv2, Seq("ctn"), "INNER")

	}

	def printExplain(df: DataFrame): Unit = {

		df.explain(true)

	}

	def changeConfigValue(spark: SparkSession, value: String): Unit = {

		spark.conf.set("spark.sql.autoBroadcastJoinThreshold", value)

	}

	def overWindows(df: DataFrame): Unit = ???



	val spark: SparkSession = sparkConnect("login")

	val csvFile1Schema = StructType(
		StructField("name", StringType, nullable = true) ::
		StructField("gender", StringType, nullable = true) :: Nil
	)
	val dfCsv1 = readCSV(spark, csvFile1Schema, "file01.csv")

	val csvFile2Schema = StructType(
		StructField("name", StringType, nullable = true) ::
		StructField("gender", StringType, nullable = true) :: Nil
	)
	val dfCsv2 = readCSV(spark, csvFile2Schema, "file02.csv")

	val dfJoin: DataFrame = joinDFs(dfCsv1, dfCsv2)
	printExplain(dfJoin)

	changeConfigValue(spark, "100mb")
	printExplain(dfJoin)



	// Вывести список сотрудников, чья заработная плата выше средней по отделу.
	// файлы пока жду от Егора


}
