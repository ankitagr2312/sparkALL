import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
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

    /*Converting rdd into indexedRowMatrix*/
    val rddWithindex1 = matrice1.zipWithIndex()
    val rows11: RDD[IndexedRow] = rddWithindex1.map {
      case (k,v) =>
        IndexedRow(v, Vectors.dense(k))
    }
    val mat = new IndexedRowMatrix(rows11)
    /*Converting indexedRowMatrix into BlockMatrix*/
    val blockMatrix1=mat.toBlockMatrix(3,2).cache()

    val rddWithindex2 = matrice2.zipWithIndex()
    val rows12: RDD[IndexedRow] = rddWithindex2.map {
      case (k,v) =>
         IndexedRow(v, Vectors.dense(k))
    }

    val mat2 = new IndexedRowMatrix(rows12)
    val blockMatrix2=mat2.toBlockMatrix(2,3).cache()

    rows11.collect().foreach(println)
    rows12.collect().foreach(println)

    println("Printing BlockMatrix rows value::::::::::::::::")
    blockMatrix1.blocks.collect().foreach(println)
    blockMatrix2.blocks.collect().foreach(println)

    /*Matrix multiplication in Distributed */

    val result = blockMatrix1.multiply(blockMatrix2)

    /*Calculate nuber of rows and columns*/
    val nCols = result.numCols()
    val nRows = result.numRows()

    /*Transpose a matrix*/
    val tranposeResult = result.transpose

    result.blocks.collect().foreach(println)
    println("========================>><<========================")
    tranposeResult.blocks.collect().foreach(println)

    /*val X = result.toIndexedRowMatrix()
    val nCoef = X.numCols.toInt
    val svd = X.computeSVD(nCoef, computeU = true)

    // Create the inv diagonal matrix from S
    val invS = DenseMatrix.diag(new DenseVector(svd.s.toArray.map(x => math.pow(x, -1))))
    val U = svd.U.toBlockMatrix().toLocalMatrix().multiply(DenseMatrix.eye(svd.U.numRows().toInt)).transpose
    val V = svd.V
    val invOfResult = (V.multiply(invS)).multiply(U)*/
    val invOfResult = computeInverse(result.toIndexedRowMatrix())
    println("Inverse of matrix=================>>>>>")
    println("numCol for inverser matrix:"+invOfResult.numCols)
    println("numRow for inverser matrix:"+invOfResult.numRows)
    invOfResult.values.foreach(println)


  }

  /*Coalculate inverse of matrix by passing Index*/
  def computeInverse(X: IndexedRowMatrix)
  : DenseMatrix =
  {
    val nCoef = X.numCols.toInt
    val svd = X.computeSVD(nCoef, computeU = true)
    if (svd.s.size < nCoef) {
      sys.error(s"IndexedRowMatrix.computeInverse called on singular matrix.")
    }

    // Create the inv diagonal matrix from S
    val invS = DenseMatrix.diag(new DenseVector(svd.s.toArray.map(x => math.pow(x, -1))))

    // U cannot be a RowMatrix
    val U = svd.U.toBlockMatrix().toLocalMatrix().multiply(DenseMatrix.eye(svd.U.numRows().toInt)).transpose

    val V = svd.V
    (V.multiply(invS)).multiply(U)
  }

}

