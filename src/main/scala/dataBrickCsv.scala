import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
/**
  * Created by tkmae6e on 14/12/16.
  */
object dataBrickCsv {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("matrix_manipulation").setMaster("local[*]"))
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    val df = hiveContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("/Users/tkmae6e/Documents/train.csv")
    println("printing json")
df.toJSON.collect().foreach(println)


    val selectedData = df.select("Date","Sales","Store","StateHoliday")
    //selectedData.collect().foreach(println)
    df.registerTempTable("temp_table")
    val tempResult = hiveContext.sql("Select Store,Date,Sales," +
      "Sales-lag(Sales) over (partition by Store) as diff_variable1," +
      "CASE WHEN  StateHoliday = '0' THEN 1 ELSE 0 END AS StateHoliday_0 ," +
      "CASE WHEN  StateHoliday = 'a' THEN 1 ELSE 0 END AS StateHoliday_a ," +
      "CASE WHEN  StateHoliday = 'b' THEN 1 ELSE 0 END AS StateHoliday_b," +
      "CASE WHEN  StateHoliday = 'c' THEN 1 ELSE 0 END AS StateHoliday_c from temp_table")
     //tempResult.collect().foreach(println)



    val windowSpec = Window.partitionBy(tempResult("Store")).orderBy(tempResult("Date"))
    val resultLag = tempResult.withColumn("SaleVar",lag(tempResult("Sales"),1) over (windowSpec))
      .withColumn("StateHoliday_varA",lag(tempResult("StateHoliday_a"),1) over (windowSpec))
      .withColumn("StateHoliday_var0",lag(tempResult("StateHoliday_0"),1) over windowSpec)
      .withColumn("StateHoliday_varB",lag(tempResult("StateHoliday_b"),1) over windowSpec)
      .withColumn("StateHoliday_varC",lag(tempResult("StateHoliday_c"),1) over windowSpec)
    resultLag.collect().foreach(println)
  }
}
