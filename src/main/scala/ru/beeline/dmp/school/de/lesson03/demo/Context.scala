package ru.beeline.dmp.school.de.lesson03.demo

import org.apache.spark.sql.SparkSession

trait Context {

  val appName: String

  private def createSession(appName: String) = {
    SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
  }

  lazy val spark = createSession(appName)

}
