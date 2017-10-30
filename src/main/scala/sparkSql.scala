import SparkJoinTest.schema
import org.apache.spark.{SparkConf, SparkContext}
import it.nerdammer.spark.hbase._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
/**
  * Created by tkmadks on 8/7/16.
  */
object sparkSql {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark_Join").setMaster("local[*]").set("spark.sql.autoBroadcastJoinThreshold","10485760"))
/*    val configuration: Configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", "10.190.144.8")
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    val hBaseContext:HBaseContext = new HBaseContext(sc, HBaseConfiguration.create(configuration))*/
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val rdd1= sc.textFile("/Users/tkmae6e/sampleDir/test1.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1).toInt,s(2).toInt,s(3).toInt))
    val df1 = rdd1.toDF("col1", "col2","col3","col4")

    sc.getConf.set("spark.hbase.host", "10.0.2.15")
    sc.getConf.set("spark.deploy.zookeeper.url", "10.0.2.15")
/*    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","10.0.2.15");
    conf.set("hbase.zookeeper.property.clientPort","2181");
    conf.set("zookeeper.znode.parent","/hbase");*/



    val someRDD = df1.rdd.map(x=> (x(0).toString,x.toString())).
      toHBaseTable("emp")
    df1.schema.fields.foreach(x => someRDD.toColumns(x.name))
    someRDD.inColumnFamily("keysCol").save()


    /*.options(Map("table" -> outputWriterOptions.tableName.get, "zkUrl" -> zookeeperURL))*/
  }
}

case class schema(col1: Int, col2: Int,col3 :Int,col4 :Int)
