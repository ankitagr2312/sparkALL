import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.EmptyRDD

/*
import akka.actor.{Actor, ActorSystem, Props}
import akka.actor.Actor.Receive
*/

/**
  * Created by tkmae6e on 22/09/16.
  */


/*class testActorClass extends Actor {
  override def receive: Receive = {
    case s:String => println(s)
    case i:Int    => println("huh?")
  }
}*/
object testActor {
  def main(args: Array[String]): Unit = {
    /*val system= ActorSystem("SimpleAkkaSystem")
    val actor = system.actorOf(Props[testActorClass],"testActorClass")
    actor ! "ankit"*/
    val sc = new SparkContext(new SparkConf().setAppName("Spark_Join").setMaster("local[*]").set("spark.sql.autoBroadcastJoinThreshold","10485760"))

    val inputrdd = sc.parallelize(
      List(
        ("maths", 21),
        ("english", 22),
        ("science", 31)
      )
    )

    val someRDD = inputrdd.map(x => (x._2,x))
    println(someRDD.getNumPartitions)
    println(someRDD.toDebugString)
    //someRDD.collect().foreach(println)
    println("**************************************************")
    //inputrdd.collect().foreach(println)
    val sortedRDD = someRDD.sortByKey()
    println("**************************************************")
    println(sortedRDD.getNumPartitions)
    println(sortedRDD.toDebugString)
    //sortedRDD.collect().foreach(println)

    //println(inputrdd.partitions.size)
    //println(inputrdd.getNumPartitions)
    val mapped =  inputrdd.mapPartitionsWithIndex{
      // 'index' represents the Partition No
      // 'iterator' to iterate through all elements
      //                         in the partition
      (index, iterator) => {
        //println("Called in Partition -> " + index)
        val myList = iterator.toList
        // In a normal user case, we will do the
        // the initialization(ex : initializing database)
        // before iterating through each element
        //myList.foreach(x => println(x))
        //println("size of index " +index+" is "+myList.size)
        myList.map(x => x + " -> " + index).iterator
      }
    }

    mapped.collect()

  }

}
