package ru.beeline.dmp.school.de.lessonFinal

import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

// 1 - наследование -----------------------------------------------------------------------
object ProjectExample extends App {

	// 2 - Подключение или создание SparkSession -----------------------------------------------------------------------
	val spark: SparkSession = SparkSession
		.builder()
		.enableHiveSupport()
		.getOrCreate()


	// 3 - Получение логина и пароля ТУЗ -------------------------------------------------------------------------------
	val urlPostgres = "jdbc:postgresql://ingress-1.prod.dmp.vimpelcom.ru:5448/demo"
	val loginPostgres = spark.conf.get("spark.postgres.login")
	val passwordPostgres = spark.conf.get("spark.postgres.password")


	// 4 - Чтение таблиц из Postgresql ---------------------------------------------------------------------------------
	val dfSeats: DataFrame = spark.read
			.format("jdbc")
			.option("driver", "org.postgresql.Driver")
			.option("url", urlPostgres)
			.option("dbtable", "bookings.seats")
			.option("user", loginPostgres)
			.option("password", passwordPostgres)
			.load()

	val dfFlights: DataFrame = spark.read
		.format("jdbc")
		.option("driver", "org.postgresql.Driver")
		.option("url", urlPostgres)
		.option("dbtable", "bookings.flights_v")
		.option("user", loginPostgres)
		.option("password", passwordPostgres)
		.load()

	// 5 - Сохранение DF на Hive ---------------------------------------------------------------------------------------
	val login = "MYuRoshchin".toLowerCase()

	dfSeats
		.coalesce(1)
		.write
		.mode(SaveMode.Overwrite)
		.format("orc") // orc vs parquet
		.option("path", s"hdfs://ns-etl/warehouse/tablespace/external/hive/school_de.db/seats_$login")
		.saveAsTable(s"school_de.seats_$login")

	dfFlights
		.withColumn("p_date", dfFlights("datetime").cast(DateType))
		.coalesce(1)
		.write
		.mode(SaveMode.Overwrite)
		.format("orc") // orc vs parquet
		.partitionBy("p_date")
		.option("path", s"hdfs://ns-etl/warehouse/tablespace/external/hive/school_de.db/flights_$login")
		.saveAsTable(s"school_de.seats_$login")

}
