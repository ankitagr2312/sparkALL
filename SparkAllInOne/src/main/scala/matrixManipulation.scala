import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
/**
  * Created by tkmae6e on 19/11/16.
  */
object matrixManipulation {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("matrix_manipulation").setMaster("local[*]"))
    val matrice1 = sc.textFile("/Users/tkmae6e/mlibSampleInputFiles/matrix1").map(_.split(",").map(_.toDouble))
    val matrice2 = sc.textFile("/Users/tkmae6e/mlibSampleInputFiles/matrix2").map(_.split(",").map(_.toDouble))
    val r = scala.util.Random

    val rows11: RDD[IndexedRow] = matrice1.map {
      line =>
        IndexedRow(line.length, Vectors.dense(line))
    }


    val mat = new IndexedRowMatrix(rows11)
    val blockMatrix1=mat.toBlockMatrix().cache()


    val rows12: RDD[IndexedRow] = matrice2.map {
      line =>
        IndexedRow(line.length, Vectors.dense(line))
    }

    val mat2 = new IndexedRowMatrix(rows12)
    val blockMatrix2=mat2.toBlockMatrix().cache()

    println("Printing indexed rows value::::::::::::::::")
    rows11.collect().foreach(println)
    rows12.collect().foreach(println)


    println("Printing BlockMatrix rows value::::::::::::::::")
    blockMatrix1.blocks.collect().foreach(println)
    blockMatrix2.blocks.collect().foreach(println)

    /*Matrix multiplication in Distributed */
    val result = blockMatrix1.multiply(blockMatrix2)

    val nCols = result.numCols()
    val nRows = result.numRows()

    result.blocks.collect().foreach(println)





  }

}

