package ru.beeline.dmp.school.de.lessonFinal.task

import org.rogach.scallop._

class CsvGeneratorFinalTaskArg(arguments: Seq[String]) extends ScallopConf(arguments) {
  /*
      event-date = 2023-10-01
      партиция с датой , за которую генерируются данные

     target-path = /data/generated_csv
      путь,куда складываются сгенерированные данные

     sample-count = 10000
     число сгенерированных строк
  */

  val eventDate: ScallopOption[String] = opt[String](required = true, name = "event-date", default = Some("2023-10-01"))
  val targetPath: ScallopOption[String] = opt[String](required = true, name = "target-path")
  val sampleCount: ScallopOption[Int] = opt[Int](required = true, name = "sample-count", default = Some(1000))

  verify()

}
