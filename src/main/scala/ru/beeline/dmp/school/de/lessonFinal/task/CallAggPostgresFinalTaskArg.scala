package ru.beeline.dmp.school.de.lessonFinal.task

import org.rogach.scallop._

// Параметры приложения
//--event-date=2023-10-01
//--target-table=avg_call_length_by_region

class CallAggPostgresFinalTaskArg(arg: Seq[String]) extends ScallopConf(arg) {

  val eventDate: ScallopOption[String] = opt[String](required = true, name = "event-date", default = Some("2023-10-01"))
  val targetTable: ScallopOption[String] = opt[String](required = true, name = "target-table")
  val urlPostgres: ScallopOption[String] = opt[String](required = true, name = "url", default = Some("jdbc:postgresql://postgresql:5432/postgres"))
  val loginPostgres: ScallopOption[String] = opt[String](required = true, name = "login", default = Some("p_user"))
  val passwordPostgres: ScallopOption[String] = opt[String](required = true, name = "password", default = Some("password123"))

  verify()
}
