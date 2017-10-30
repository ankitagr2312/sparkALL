

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import java.sql.DriverManager

import scalikejdbc.DB

/**
  * Created by tkmae6e on 18/05/17.*/

object KafkaExactlyOnce {

  val dbURL = "jdbc:derby://localhost:1527/myDB;create=true;"
  val tableName = "kafka"
  Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
  val conn = DriverManager.getConnection(dbURL);
  val stm = conn.createStatement()

  def main(args: Array[String]): Unit = {


    val ssc = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("ExactlyOnce"), Seconds(60))
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092",
      "client.id"->"KafkaProducer",
      "key.serializer"->"org.apache.kafka.common.serialization.IntegerSerializer",
      "value.serializer"->"org.apache.kafka.common.serialization.StringSerializer")
    val topics = Set("test4")

    val appName = ssc.sparkContext.appName.toString
    val result = stm.executeQuery(s"select appName,topicName,partitionVal,offset from app.KAFKATEST where appName='$appName'")
    var fromOffsets= Map[TopicAndPartition,Long]()

    while(result.next())
      {
        fromOffsets += TopicAndPartition(result.getString("topicName").toString,result.getString("partitionVal").toInt) -> result.getString("offset").toLong
      }

    val stream = if (fromOffsets.size==0) KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics) else KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String)](
      ssc, kafkaParams, fromOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.message()))

    stream.print()

    //println(stream)
    stream.foreachRDD(rdd =>{
      rdd.collect().foreach(println)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //offsetRanges.foreach(println)
      rdd.foreachPartition{x =>
        //x.foreach(println)
        val osr: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        //println(osr.untilOffset)
        val untilOffset=osr.untilOffset.toString
        val topic = osr.topic.toString
        val kafkaPartitionId = osr.partition.toString

        updateTable(appName,topic,kafkaPartitionId,untilOffset)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }


  def updateTable(col1:String,col2:String,col3:String,col4:String) :Unit=
  {
    stm.executeUpdate(s"DELETE FROM app.KAFKATEST where appName='$col1' and topicName='$col2' and partitionval='$col3'")
    stm.executeUpdate(s"Insert into  app.KAFKATEST values('$col1','$col2','$col3','$col4')")
  }
}

/*
*
*     props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
*/


/*
*
* INSERT INTO tblExample (exampleColumn)
SELECT  Value FROM (  SELECT 'test' Value) T1
WHERE NOT EXISTS (SELECT 1 FROM tblExample where exampleColumn = 'test')
* */