import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tkmadks on 8/7/16.
  */
object sparkSql {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("spark_sql").setMaster("local[*]"))
    val rdd1 =sc.textFile("/Users/tkmadks/Documents/test.txt")
    rdd1.collect().foreach(println)
  }
}
