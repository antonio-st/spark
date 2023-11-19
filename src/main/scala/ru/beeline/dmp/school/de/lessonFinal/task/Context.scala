package ru.beeline.dmp.school.de.lessonFinal.task

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
