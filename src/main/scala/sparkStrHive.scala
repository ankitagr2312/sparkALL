import org.apache.spark.{SparkConf, SparkContext}

object sparkStrHive {

  def main(args: Array[String]): Unit = {



    val sc = new SparkContext(new SparkConf().set("","")
      .setMaster("local")
      .setAppName("myApp"))
  }
}
