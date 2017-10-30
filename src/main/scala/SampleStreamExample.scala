
import java.nio.charset.Charset

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SampleStreamExample {
  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("SampleStreamerProgram").setMaster("local[*]")
    val ssc = new StreamingContext(config, Seconds(10))

    val lines = ssc.socketTextStream("localhost", 4141)
    lines.foreachRDD(rdd => rdd.foreach(println))

    val extractData = lines.flatMap(str => createRecord(str))
    val mapExtractedData = extractData.map(row => (row(2).asInstanceOf[String],row))
    val grpExtractedData = mapExtractedData.groupByKey()
    val sortedExtractedData = grpExtractedData.mapValues(itr => itr.toList.sortWith(sortByTimestamp))
    /*sortedExtractedData.foreachRDD(rdd => rdd.foreach(grp => {
      print("GroupName: "+grp._1.toString+" ")
      grp._2.foreach(row => {
        for(i <- 0 to row.size-1)
          print(" "+row(i)+" ")
      })
      println()
    }))*/

    /*
    * pageName, placement, bunddleId
    * */

    val filteredData = sortedExtractedData.flatMap(grp => {
      var pageName = ""
      var placement = ""
      var bunddleId = ""
      var flag =false
              val someVAl:List[Row] = grp._2.map(row => row match {
                case _ if(row(0).asInstanceOf[String]== "rec_received") =>
                  {
                    pageName=row(4).asInstanceOf[String]
                    placement=row(5).asInstanceOf[String]
                    bunddleId=row(6).asInstanceOf[String]
                    flag=true
                    row
                  }
                case _ if(row(0).asInstanceOf[String]== "rec_product_click"&& flag) =>
                  {
                    Row(row(0).asInstanceOf[String],row(1).asInstanceOf[Long],row(2).asInstanceOf[String]
                      ,row(3).asInstanceOf[String],pageName,placement,
                      bunddleId,row(7).asInstanceOf[String],row(8).asInstanceOf[Integer])
                  }
                case _ if(!flag) => row
              })
      someVAl
    })

    filteredData.foreachRDD(rdd => rdd.foreach(row => {
      for(i <- 0 to row.size-1)
        print(" "+row(i)+" ")
      println()
    }
    ))


    /*
    * val mappedLineSTream = lines.map(_.split(",")).map(x => (x(1),x))
    * val grpLineSTream = mappedLineSTream.groupByKey()
    * val someVal = grpLineSTream.mapValues(itr => itr.toList.sortWith(sortByLength))
    * someVal.foreachRDD(rdd => rdd.collect().foreach(x => {
    *    print(x._1+"     ")
    *    x._2.foreach(x => x.foreach(y => print(" "+y+" ")))
    *    println()
    *       }
    *     )
    *    )
    * */

      ssc.start()
      ssc.awaitTermination()
    }

     def createRecord(value: String): Seq[Row] = {
      var records= Seq.empty[Row]

      for { message <- value.split('␝')
      } {
        val pairs = message.split('␞')

        if((pairs(7).split('␟')(1)) == "rec_received") {
          val epochTime = (pairs(0).split('␟')(1)).toLong
          val eventType = (pairs(7).split('␟')(1))
          val correlation_id = (pairs(2).split('␟')(1))
          val channel = (pairs(8).split('␟')(1))
          val pageName = (pairs(15).split('␟')(1))
          val placement = (pairs(16).split('␟')(1))
          val bunddleId = (pairs(11).split('␟')(1))
          val metric = (pairs(12).split('␟')(1))

          records = records :+ Row(eventType,epochTime ,correlation_id, channel, pageName, placement, bunddleId, metric, 1)
        }
        else if((pairs(7).split('␟')(1)) == "rec_product_click") {
          val epochTime = (pairs(0).split('␟')(1)).toLong
          val eventType = (pairs(7).split('␟')(1))
          val correlation_id = (pairs(2).split('␟')(1))
          val channel = ""
          val pageName = ""
          val placement = ""
          val bunddleId = ""
          val metric = "CTR"      // Metric is fixed as CTR

          records = records :+ Row(eventType,epochTime,correlation_id, channel, pageName, placement, bunddleId, metric, 1)
        }
      }
      records
  }

  def sortByTimestamp(s1: Row, s2: Row) = {
    val rowField = s1.get(2).toString
    val jn = s2.get(2)
    println("Field Value is ::: "+ s1.get(2).toString)
    println("comparing %s and %s".format(s1(3), s2(3)))
    s1.get(1).asInstanceOf[Long] <  s2.get(1).asInstanceOf[Long]
  }
}