
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter.TwitterUtils
/**
  * Collect at least the specified number of tweets into json text files.
  */
object SampleStreamExample {
  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("SampleStreamerProgram")
    val ssc = new StreamingContext(config, Seconds(2))

    val lines = ssc.textFileStream("file:///root/newDir/")
    lines.foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }


}