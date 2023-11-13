ThisBuild / version := "1.0"

val sparkVersion = "3.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark-scala"
  )

scalacOptions += "-Ypartial-unification"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "com.typesafe" % "config" % "1.4.2",
  "org.rogach" %% "scallop" % "4.1.0",
  "org.postgresql" % "postgresql" % "42.6.0"
)


assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
