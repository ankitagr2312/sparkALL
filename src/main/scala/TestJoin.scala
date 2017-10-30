import SparkJoinTest.schema
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import org.apache.spark.sql
//import com.redislabs.provider.redis._
import com.redis._


import scala.collection.mutable.ListBuffer
/**
  * Created by tkmae6e on 30/05/17.
  */
object TestJoin extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf()
      .setMaster("local")
      .setAppName("myApp"))

      // initial redis host - can be any node in cluster mode
      //.set("redis.host", "localhost")

      // initial redis port
      //.set("redis.port", "6379"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._


    val rdd1 = sc.parallelize(List(
      ("requirepass","15"),
      ("mixdigest","2"),
      ("modifies","1")))

    val rdd7 = sc.parallelize(List(
      ("requirepass","15"),
      ("mixdigest","2"),
      ("modifies","1")))
    rdd7.toDebugString

    println("Number of partitioner ::"+rdd7.getNumPartitions)

    rdd7.foreachPartition(x => x.foreach(println))
    rdd7.collect().foreach(println)

    val arr:Array[(String, String)] = Array(("requirepass","15"),
      ("mixdigest","2"), ("propagte","1"), ("used_cpu_sys","1"), ("rioFdsetRead","2"), ("0x3e13","1"),
      ("preventing","1"), ("been","12"), ("modifies","1"), ("geoArrayCreate","3"))
    val redisDB = ("127.0.0.1", 6379)


    val r = new RedisClient("localhost", 6379)/*
    val putData = sc.toRedisZSET(rdd1, "all:words")
    sc.toRedisHASH(rdd1,"jnj")*/

    val rdd2= sc.textFile("/Users/tkmae6e/sampleDir/test1.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1).toInt,s(2).toInt,s(3).toInt))
    val df2 = rdd2.toDF("col5", "col6","col3","col4")
    df2.collect().foreach(println)


    val peopleArray = df2.collect.map(r => Map(df2.columns.zip(r.toSeq):_*))
    val schemaList = df2.schema.map(_.name).zipWithIndex
    println(schemaList.toMap.get("col4").get.toInt)
     val primaryKeyIndex:Int = if (schemaList.toMap.get("col4").isDefined)schemaList.toMap.get("col4").get.toInt
    else
       0



/*


    df2.mapPartitions{partition =>
      val r = new RedisClient("192.168.1.101", 6379)
      val res = partition.map { x =>
        val map = scala.collection.mutable.Map[String,String]()
        schemaList.map(rec => map+=((rec._1.toString ->x(rec._2).toString)))
        r.hmset(rx(primaryKeyIndex).toString, map)
      }
    }
*/

    /*
    * val perhit = perhitFile.mapPartitions{partition =>
    val r = new RedisClient("192.168.1.101", 6379) // create the connection in the context of the mapPartition operation
    val res = partition.map{ x =>
        ...
        val refStr = r.hmget(...) // use r to process the local data
    }
    r.close // take care of resources
    res
}*/
//val arr = new List[String,Map[String,String]]

  /* val rr=df2.rdd.map(row => {
      //here rec._1 is column name and rce._2 index
     println(row)
     val map = scala.collection.mutable.Map[String,String]()
      schemaList.map(rec => map+=((rec._1.toString ->row(rec._2).toString)))
     (row(primaryKeyIndex).toString,map)

     //
     //val rdd51 = sc.parallelize(colHir)
     //submitDataToRedis(sc,colHir,row(primaryKeyIndex).toString)
    }).collect().toList*/


    df2.foreachPartition(partition => {
      val rt = new RedisClient("localhost", 6379)
      partition.foreach(row => {
        val map = scala.collection.mutable.Map[String,String]()
        schemaList.map(rec => map+=((rec._1.toString ->row(rec._2).toString)))
        rt.hmset(row(primaryKeyIndex).toString,map)
      }
      )
    })






    println("ndnjdnk")
  }


  def submitDataToRedis(sc:SparkContext, listB:ListBuffer[(String,String)], primaryKeyIndex:String) ={
    val rdd1 = sc.parallelize(listB)
    //sc.toRedisHASH(rdd1,primaryKeyIndex)
  }
}
