name := "SparkAllInOne"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-core_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-hive_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-launcher_2.10" % "1.6.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.3",
  "org.scala-lang" % "scala-actors" % "2.10.0-M7",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1",
  "com.databricks" % "spark-csv_2.10" % "1.5.0",
  "org.parboiled" % "parboiled_2.10" % "2.0-M1",
  "io.spray" % "spray-json_2.10" % "1.3.3",
  "com.google.code.gson" % "gson" % "2.8.0",
  "com.typesafe.play".%("play_2.10") % "2.4.0-M1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.0",
  "org.scalikejdbc" % "scalikejdbc_2.10" % "3.0.0-M4",
  "org.apache.derby" % "derby" % "10.13.1.1",
  "org.apache.derby" % "derbyclient" % "10.13.1.1",
  "redis.clients" % "jedis" % "2.8.1",
  "net.debasishg" %% "redisclient" % "3.4",
  "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3"/*,
  "org.apache.hbase" % "hbase-spark" % "1.1.2.2.6.0.0-598"*/
)
unmanagedJars in Compile += file(Path.userHome+"/Users/tkmae6e/jars/sparktimeseries-0.1.0-jar-with-dependencies.jar")
unmanagedJars in Compile += file(Path.userHome+"/Users/tkmae6e/Downloads/spark-redis-0.3.2.jar")

