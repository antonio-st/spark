package ru.beeline.dmp.school.de.lesson03.demo


import org.apache.spark.internal.Logging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window


object WindowFunctionsDemo extends App with Logging {
  //
  //  Logger.getRootLogger.setLevel(Level.WARN)
  //  val logger: Logger = Logger.getLogger(getClass.getName)
  //  logger.setLevel(Level.INFO)

  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark: SparkSession = SparkSession
    .builder()
    .appName("Lesson_3_WindowFunctions")
    .enableHiveSupport()
    .getOrCreate()


  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )

  val df = simpleData.toDF("employee_name", "department", "salary")


  //row_number
  //группируем по department и сортируем по salary, и нумеруем последовательно
  val windowSpec = Window.partitionBy("department").orderBy("salary")
  df.withColumn("row_number", row_number.over(windowSpec))
    .show()

  //rank
  //  1 1 3 3 5
  //группируем по department и сортируем по salary, и нумеруем последовательно,
  // но дубликатах salary числа не меняются
  df.withColumn("rank", rank().over(windowSpec))
    .show()

  // dense_rank
  // 1 1 2 2 3
  // аналогично rank, но порядок не сохраняется

  df.withColumn("dense_rank", dense_rank().over(windowSpec))
    .show()

  //lag
  // в колонке lag значение salary смещенное на 2 пункта, в начале null
  df.withColumn("lag", lag("salary", offset = 2).over(windowSpec))
    .show()

  //aggregate_functions
  //
  val windowSpecAgg = Window.partitionBy("department")
  val aggDF = df.withColumn("row", row_number.over(windowSpec))
    .withColumn("avg", avg("salary").over(windowSpecAgg))
    .withColumn("sum", sum("salary").over(windowSpecAgg))
    .withColumn("min", min("salary").over(windowSpecAgg))
    .withColumn("max", max("salary").over(windowSpecAgg))
    .where($"row" === 1).select("department", "avg", "sum", "min", "max")
    .show()

}