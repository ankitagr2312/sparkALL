import SparkJoinTest.schema
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.MurmurHash
import scala.util.hashing.MurmurHash3


/**
  * Created by tkmae6e on 08/06/17.
  */
object graphX {
  def main(args: Array[String]): Unit = {



    val sc = new SparkContext(new SparkConf().setAppName("Spark_Join").setMaster("local[*]").set("spark.sql.autoBroadcastJoinThreshold","10485760"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val df_1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/tkmae6e/Downloads/2008.csv")

    val flightsFromTo = df_1.select($"Origin",$"Dest")
    val airportCodes = df_1.select($"Origin", $"Dest").flatMap(x => Iterable(x(0).toString, x(1).toString))
    val airportVertices: RDD[(VertexId, String)] = airportCodes.distinct().map(x => (MurmurHash.stringHash(x).asInstanceOf[VertexId],x))
    val defaultAirport = ("Missing")
    val flightEdges = flightsFromTo.map(x =>
      ((MurmurHash.stringHash(x(0).toString),MurmurHash.stringHash(x(1).toString)), 1)).reduceByKey(_+_).map(x => Edge(x._1._1, x._1._2,x._2))
    val graph = Graph(airportVertices, flightEdges, defaultAirport)
    println("Number of Partititons "+flightEdges.getNumPartitions)
    println("Debug String "+flightEdges.toDebugString)
    //println(graph.numVertices)
      //println(graph.numEdges)
    //flightEdges.take(10).foreach(println)

    val somehbhb = graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
      "There were " + triplet.attr.toString + " flights from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10)
    //somehbhb.foreach(println)
  }
  case class schema(col1: Int, col2: Int,col3 :Int,col4 :Int)
}
