import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tkmae6e on 06/12/16.
  */
object timeSeries {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark_Join").setMaster("local[*]").set("spark.sql.autoBroadcastJoinThreshold","10485760"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  }
}
