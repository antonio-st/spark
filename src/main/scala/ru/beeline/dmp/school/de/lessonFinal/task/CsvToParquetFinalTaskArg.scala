package ru.beeline.dmp.school.de.lessonFinal.task

import org.rogach.scallop._

// Параметры прииложения
// --event-date=2023-10-01
// --target-path=/data/generated_parquet

class CsvToParquetFinalTaskArg(arguments: Seq[String]) extends ScallopConf(arguments) {

  val eventDate: ScallopOption[String] = opt[String](required = true, name = "event-date", default = Some("2023-10-01"))
  val targetPath: ScallopOption[String] = opt[String](required = true, name = "target-path")

  verify()


}
