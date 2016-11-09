name := "benchmark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val gatlingVersion = "2.2.2"
  val configVersion = "1.3.1"
  Seq(
    "io.gatling.highcharts" % "gatling-charts-highcharts" % gatlingVersion,
    "com.typesafe" % "config" % configVersion)
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}