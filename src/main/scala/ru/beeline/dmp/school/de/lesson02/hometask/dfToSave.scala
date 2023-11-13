package ru.beeline.dmp.school.de.lesson02.hometask

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object dfToSave extends App with Logging {
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("home_task_spark_lesson2")
    .enableHiveSupport()
    .getOrCreate()

  // прочитаем csv , используем inferSchema для последующего чтения схемы

  val dfShop = spark
    .read.option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/events_data_csv/2021-11-25")

  dfShop.show(5)

  dfShop.printSchema()

  // 2. Прочитать данные в формате csv, задав строго схему данных в файле с помощью
  //   StructType(Array( StructField(...), ...   ))


  val schemaShop = StructType(Array(
    StructField("event_time", DateType, true),
    StructField("event_type", StringType, true),
    StructField("product_id", IntegerType, true),
    StructField("category_id", LongType, true),
    StructField("category_code", StringType, true),
    StructField("brand", StringType, true),
    StructField("price", DoubleType, true),
    StructField("user_id", IntegerType, true),
    StructField("user_session", StringType, true),
    StructField("date", DateType, true)
  )
  )

  val dfShopShema = spark
    .read
    .option("header", "true")
    .schema(schemaShop)
    .csv("/data/events_data_csv/2021-11-25")

  dfShopShema.printSchema()


  // 3. Написать аггрегаты продаж товаров по категориям и сохранить результат в формате parquet.

  import spark.implicits._

  val dfShopShemaAgg = dfShopShema
    .groupBy($"category_code")
    .agg(max($"price").as("max_price"),
      min($"price").as("min_price"),
      round(avg($"price"), 2).as("avg_price"),
      round(sum($"price"), 2).as("sum_of_price")
    )
    .orderBy("category_code")

  dfShopShemaAgg.show(20, 100)

  // dfShopShema.withColumn("category_code_cls", trim($"category_code")).drop("category_code").show()

  dfShopShemaAgg
    .write
    .mode("overwrite")
    .format("parquet")
    .save("/data/shop_category_task_2")

}
