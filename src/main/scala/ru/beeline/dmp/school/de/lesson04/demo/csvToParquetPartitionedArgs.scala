package ru.beeline.dmp.school.de.lesson04.demo

import org.rogach.scallop._

class csvToParquetPartitionedArgs(arguments: Seq[String]) extends ScallopConf(arguments) {

  val sourcePath: ScallopOption[String] = opt[String](required = true, name = "source-path")
  val targetPath: ScallopOption[String] = opt[String](required = true, name = "target-path")
  val loadDate: ScallopOption[String] = opt[String](required = true, name = "load-date")

  verify()
}
