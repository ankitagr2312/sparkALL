import java.util.Calendar

import SparkJoinTest.schema
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tkmae6e on 05/06/17.
  */
object LearnProgram {
  def main(args: Array[String]): Unit = {

    val now = Calendar.getInstance().getTime()
    println(now)
    val sc = new SparkContext(new SparkConf().
      setAppName("Spark_Join").setMaster("local[*]"))
      //.set("spark.sql.autoBroadcastJoinThreshold","10485760"))
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //import sqlContext.implicits._
    val kfyufg= sc.textFile("/Users/tkmae6e/sampleDir/test1.csv").map(_.split(","))

    //kfyufg.collect().foreach(println)

    val employeeData = List(("Jack",1000.0),("Bob",2000.0),("Carl",7000.0))
    val employeeRDD = sc.makeRDD(employeeData,1)
    //println("Number of partitions "+employeeRDD.getNumPartitions)
    //println("Data in each partition")
    var counterq =0
    employeeRDD.foreachPartition(x => {
      //println("Partition number :"+counterq)
      counterq=counterq+1
      x.foreach(println)
    })
    val dummyEmployee = ("dummy",0.0)
    val maxSalaryEmployee = employeeRDD.fold(dummyEmployee)((acc,employee) => {
      //println("Accumulator value "+acc._2)
      //println("Employee value value "+employee._2)
      if(employee._2 - acc._2 >= 1000) {
        employee
      }
      else
        acc
    }
    )
    val someRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    val inputrdd = sc.parallelize(
      List(
        (123, 21),
        (1234, 22),
        (12345, 31),
        (123, 21),
        (1234, 22),
        (12345, 31)
      ),
      5
    )

    inputrdd.partitions.size
    val result = inputrdd.aggregate(3) (
      /*
       * This is a seqOp for merging T into a U
       * ie (String, Int) in  into Int
       * (we take (String, Int) in 'value' & return Int)
       * Arguments :
       * acc   :  Reprsents the accumulated result
       * value :  Represents the element in 'inputrdd'
       *          In our case this of type (String, Int)
       * Return value
       * We are returning an Int
       */
      (acc, value) => (acc + value._2),

      /*
       * This is a combOp for mergining two U's
       * (ie 2 Int)
       */
      (acc1, acc2) => (acc1 + acc2)
    )

    val resultOfReducedByKey =inputrdd.reduceByKey((x,y) => x+y)
    println("Aggregated value :"+result)
    resultOfReducedByKey.collect().foreach(println)
    resultOfReducedByKey.sortByKey(true).collect().foreach(println)
    println("Size of RDD "+SizeEstimator.estimate(resultOfReducedByKey))
    val u = (12322L-233L)/100

    println("some hbvhbvhbh :"+u)


    //println("employee with maximum salary is"+maxSalaryEmployee)

// < employee._2 - acc._2




/*    val rdd1= sc.textFile("/Users/tkmae6e/sampleDir/test1.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1).toInt,s(2).toInt,s(3).toInt))
    val df1 = rdd1.toDF("col1", "col2","col3","col4")*/




    /*val rdd2= sc.textFile("/Users/tkmae6e/sampleDir/test2.csv").map(_.split(",")).map(s => schema(s(0).toInt,s(1).toInt,s(2).toInt,s(3).toInt))
    val df2 = rdd2.toDF("col5", "col6","col3","col4")*/
  }
}
