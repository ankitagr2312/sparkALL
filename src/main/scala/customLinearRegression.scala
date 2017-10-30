import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tkmae6e on 01/12/16.
  */
object customLinearRegression {
  def main(args: Array[String]): Unit = {

    /*Initiate Spark Conf*/
    val sc = new SparkContext(new SparkConf().setAppName("linearRegression").setMaster("local[*]"))
    /*Load csv file */
    val trainRddWithHeader = sc.textFile("/Users/tkmae6e/Documents/train.csv")
    /*Remove header*/
    val trainRdd = trainRddWithHeader.mapPartitionsWithIndex(
      (i, iterator) =>
        if (i == 0 && iterator.hasNext) {
          iterator.next
          iterator
        } else iterator)

    /*Compute inv(X.transpose * X) * (X * Y) */

    val salesMatrix = convertToBlockMatrix(trainRdd.map(_.split(",")(3)))
    val promosMatrix = convertToBlockMatrix(trainRdd.map(_.split(",")(6))).transpose
    val salesMatrixTranspose = salesMatrix.transpose
    val product1 = computeInverse((salesMatrix.multiply(salesMatrixTranspose)).toIndexedRowMatrix())
    val product2 = salesMatrix.multiply(promosMatrix)
    val product = product2.toIndexedRowMatrix().multiply(product1)

    /*Print value*/
    println(product.numCols())
    println(product.numRows())
    product.toBlockMatrix().blocks.collect().foreach(println)
  }



  def convertToBlockMatrix(inputRDD :RDD[String]) : BlockMatrix = {

    val rddWithindex1 = inputRDD.map(_.split(",").map(_.toDouble)).zipWithIndex()
    val indexedRow: RDD[IndexedRow] = rddWithindex1.map {
      case (k,v) =>
        IndexedRow(v, Vectors.dense(k))
    }
    val mat = new IndexedRowMatrix(indexedRow)
    val blockMatrix=mat.toBlockMatrix()
    blockMatrix
  }

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