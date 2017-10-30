import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object CustomPartitioning {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark_Join").setMaster("local[1]"))
    val rdd =  sc.textFile("/Users/tkmae6e/Projects/scalastyle-config.xml")

    rdd.collect().foreach(println)
    println("Sytem number of cores  "+ Runtime.getRuntime().availableProcessors())
    val pairedData = rdd.map(str => (str,1))
    pairedData.collect()
  }
}

class CustomPartitioner extends Partitioner{
  override def numPartitions: Int = ???

  override def getPartition(key: Any): Int = ???
}
