
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SampleStreamingExample {
  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("SampleStreamerProgram").setMaster("local[*]")

    val ssc = new StreamingContext(config, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 4141)
    var counter = 0



    ssc.start()
    ssc.awaitTermination()
  }

  def sortByLength(s1: Array[String], s2: Array[String]) = {
    println("comparing %s and %s".format(s1(3), s2(3)))
    s1(3) > s2(3)
  }
}


case class SchemaSample (asd : String,bsd:String,csd:String)