/*
import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
//import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.Executors
import scala.concurrent._


/**
  * Created by tkmae6e on 26/04/17.
  */
object Test {
  def main(args: Array[String]): Unit = {

    implicit val ec = new ExecutionContext {
      val threadPool = Executors.newFixedThreadPool(1000);

      def execute(runnable: Runnable) {
        threadPool.submit(runnable)
      }

      def reportFailure(t: Throwable) {}
    }


    // used by 'time' method
    implicit val baseTime = System.currentTimeMillis

    // 2 - create a Future
    for ( i <- 1 to 100) {
      val f = Future(ec) {
        //Thread.sleep(5)
        println(i)

        for(j <- 200 to 210)
          println(j)
      }
      println("njn"+i)
      val result = Await.result(f, 1000 microsecond)
      //println(result)
    }


    println("jnjn")
    // 3 - this is blocking (blocking is bad)
    //val result = Await.result(f, 1 second)
    // println(result)
    //Thread.sleep(1000)
  }
}

case class ankit (vhvh : String, dfdfc : String)

//    /Users/tkmae6e/Downloads/test_26_04.csv


*/
