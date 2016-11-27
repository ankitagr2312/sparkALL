name := "SparkAllInOne"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-hive_2.10" % "1.6.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-launcher_2.10" % "1.6.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.1",
  "org.scala-lang" % "scala-actors" % "2.10.0-M7",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.0"

)

