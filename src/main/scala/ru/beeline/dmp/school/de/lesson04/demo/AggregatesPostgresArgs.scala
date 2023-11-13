package ru.beeline.dmp.school.de.lesson04.demo

import org.rogach.scallop._

class AggregatesPostgresArgs(arguments: Seq[String]) extends ScallopConf(arguments) {

  val sourcePath: ScallopOption[String] = opt[String](required = true, name = "source-path")
  val targetTable: ScallopOption[String] = opt[String](required = true, name = "target-table")
  val loadDate: ScallopOption[String] = opt[String](required = true, name = "load-date")
  val urlPostgres: ScallopOption[String] = opt[String](required = true, name = "url", default = Some("jdbc:postgresql://postgresql:5432/postgres"))
  val loginPostgres: ScallopOption[String] = opt[String](required = true, name = "login", default = Some("p_user"))
  val passwordPostgres: ScallopOption[String] = opt[String](required = true, name = "password", default = Some("password123"))

  verify()

}
