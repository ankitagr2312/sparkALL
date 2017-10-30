import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
//import org.apache.spark.broadcast
import org.apache.spark.sql.functions.broadcast


/**
  * Created by tkmae6e on 10/05/17.
  */
object SparkJoinTest {
  def main(args: Array[String]): Unit = {

    val now = Calendar.getInstance().getTime()
    println(now)
    val sc = new SparkContext(new SparkConf().setAppName("Spark_Join").setMaster("local[*]").set("spark.sql.autoBroadcastJoinThreshold","10485760"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
/*    val rdd3 = sc.textFile("/Users/tkmae6e/sampleDir/test.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1),s(2).toInt,s(3).toInt)).keyBy(s => (s.pid,s.name))
    val rdd4 = sc.textFile("/Users/tkmae6e/sampleDir/test.csv").map(_.split(",")).keyBy(s => (s(0),s(1)))*/



    val str = Array(1,2)
/*    val rdd1 = sc.textFile("/Users/tkmae6e/sampleDir/test.csv").map(_.split(",")).keyBy(a => str.length match {
      case 1 => a(str(0))
      case 2 => (a(str(0)),a(str(1)))
      case 3 => (a(str(0)),a(str(2)),a(str(3)))
    })
    //    val rdd1 = sc.textFile("/Users/tkmae6e/sampleDir").map(_.split(",")).keyBy(a => (a(0),a(1))).map(x => schema(x(0).toInt,x(1)))
    val brRdd1 = sc.broadcast(rdd1.collect())
    //val joiner = rdd1.join(rd)*/

    /*
    *
    * Join Test
    * */
    /*
    * 1.Fixed Type
    * */

    val rdd1= sc.textFile("/Users/tkmae6e/sampleDir/test1.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1).toInt,s(2).toInt,s(3).toInt))

    val df1 = rdd1.toDF("col1", "col2","col3","col4")


    val df6 = df1.flatMap(x => Iterable(x(0).toString,x(1).toString))


    val rdd2= sc.textFile("/Users/tkmae6e/sampleDir/test2.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1).toInt,s(2).toInt,s(3).toInt))
    val df2 = rdd2.toDF("col5", "col6","col3","col4")

    //df2.schema.fields.foreach(println)

    val df4 = (df2.withColumnRenamed("col5","col1")).withColumnRenamed("col6","col2")

    df2.withColumnRenamed("col6","col2")
    //df4.printSchema()
    val rdd3= sc.textFile("/Users/tkmae6e/sampleDir/test.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1).toInt,s(2).toInt,s(3).toInt))
    val df3 = rdd3.toDF("col1", "col2","col3","col4")
    /*val joinedDF = df1.join(df2, df1("col1") === df2("col1"), "leftsemi" +
      "")*/
    //val joinedDF=df1.join(broadcast(df2),df1("col1")===df2(StringToColumn()),"inner")

    val seqCol = Seq("col1", "col2","col3","col4")

    val joinedDF1=df1.as('df1).join(broadcast(df4).as('df4),usingColumns = seqCol,"inner")

//joinedDF1.collect().foreach(println)
/*
    joinedDF.collect().foreach(println)
    joinedDF.take(10).foreach(println)
    joinedDF.printSchema()*/



    val currentTime = Calendar.getInstance().getTime()
    //println(currentTime)

  }
  case class schema(col1: Int, col2: Int,col3 :Int,col4 :Int)
}


/*
val parsedDS =  ssc.sparkContext.textFile(currentDS.path).map(record=>record.split(currentDS.fieldSeperator))
parsedDS.keyBy(record=> record match  {
  case Array(a) => (record(a))
  case Array(a,b) => (a,b)
  case Array(a,b,c) => (a,b,c)
})*/
