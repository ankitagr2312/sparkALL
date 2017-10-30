import SparkJoinTest.schema
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object joinTestCases {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark_Join")
      .setMaster("local[*]").set("spark.sql.autoBroadcastJoinThreshold","10485760"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val rdd1= sc.textFile("/Users/tkmae6e/sampleDir/test1.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1).toInt,s(2).toInt,s(3).toInt))
    val df1 = rdd1.toDF("col1", "col2","col3","col4")

    val rdd2= sc.textFile("/Users/tkmae6e/sampleDir/test3.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1).toInt,s(2).toInt,s(3).toInt))
    val df2 = rdd2.toDF("col1", "col6","col3","col4")

    val conditionJoin = Seq(JoinCondition("col1","col1"),JoinCondition("col2","col6"))
    df2.printSchema()
    val newdf2 = renameColumns(df2,conditionJoin)
    newdf2.printSchema()
    val joinedDF1=df1.as('df1).join(broadcast(newdf2).as('df2),conditionJoin.map(conditin => conditin.transformationJoinField),"inner")
    joinedDF1.foreach(currentDF => println(currentDF.toString()))

  }
  private def renameColumns(dataFrame: DataFrame, joinConditions: Seq[JoinCondition]): DataFrame = {
    joinConditions headOption match {
      case Some(joinCondition: JoinCondition) => renameColumns(
        dataFrame.withColumnRenamed(joinCondition.dataSetJoinField, joinCondition.transformationJoinField),
        joinConditions.drop(1))
      case None => dataFrame
    }
  }

  case class schema(col1: Int, col2: Int,col3 :Int,col4 :Int)

  case class JoinCondition(transformationJoinField: String,
    dataSetJoinField: String)
}
