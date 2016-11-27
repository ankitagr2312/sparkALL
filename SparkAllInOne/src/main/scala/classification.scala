/**
  * Created by tkmae6e on 17/11/16.
  */


  import java.io.File
  import java.nio.file.{Files, Paths}

  import org.apache.commons.io.FileUtils
  import org.apache.spark.mllib.evaluation.MulticlassMetrics
  import org.apache.spark.mllib.linalg.Vectors
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.tree.DecisionTree
  import org.apache.spark.{SparkConf, SparkContext}

  /**
    * Created by ankit on 25/4/16.
    */


  object classification {

    //Configuration for spark
    val conf = new SparkConf().setAppName("classification").setMaster("local")
    val sc = new SparkContext(conf)

    //Function to convert string to double
    def str2Double(x: String) : Double = {
      try {
        x.toDouble
      } catch {
        case e: Exception => 0.0
      }
    }

    //function to convert each line into a Labeledpoint variable
    def parsePassengerDataToLP(inpLine : String) : LabeledPoint = {

      val values = inpLine.split(',')

      //Converting values into numerica so that feature column can be
      val count_login = values(2).toInt
      val count_active = values(3).toInt
      var lib_recd = 0
      if(values(4).trim == "YES"){
        lib_recd= 1
      }
      val gpa = str2Double(values(5))
      var grade = values(6)
      var gender = 0

      //Genrating groups
      if(values(7).trim() == "F")
      {
        gender = 1
      }
      var groups = 1
      if(values(6).trim() == "A" || values(6).trim() == "A+")
      {
        groups= 0
      }
      else if(values(6).trim() == "D" || values(6).trim() == "W")
      {
        groups = 2
      }
      return new LabeledPoint(groups,Vectors.dense(count_login,count_active,lib_recd,gender))
    }


    def main(args: Array[String]) {

      val inputData_file_path="/home/ankit/data_mlib.csv"
      val outputData_file_path="/home/ankit/Documents/output_pred.txt"

      //Checks if output directory exists.If exists than delete it
      if(Files.exists(Paths.get("/tmp")))
      {
        FileUtils.deleteDirectory(new File(outputData_file_path))
      }
      //Loading csv file through spark
      val dataFile = sc.textFile(inputData_file_path)


      //Splitting RDD into train as well as test data
      val splits = dataFile.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))

      //Making  LabledPoint RDD for train data
      val trainingDataDDLP = trainingData.map(_.trim).filter( _.length > 1).filter(_.split(",").length == 8).filter(_.split(",").toList.filter(_.nonEmpty).size ==8).
        map(line => parsePassengerDataToLP(line))


      //Building decision tree model
      val categoricalFeaturesInfo = Map[Int, Int]()
      val mdlTree = DecisionTree.trainClassifier(trainingDataDDLP, 3, //       numClasses
        categoricalFeaturesInfo, // all features are continuous
        "entropy", // impurity
        5, // Maxdepth
        32) //maxBins


      //Making LabledPoint RDD for test data
      val testDataDDLP = testData.map(_.trim).filter( _.length > 1).filter(_.split(",").length == 8).filter(_.split(",").toList.filter(_.nonEmpty).size ==8).
        map(line => parsePassengerDataToLP(line))

      //Making an RDD consisting of feature columns,lable column(actual value column) and predicted value so that it can be saved
      val output_prediction=testDataDDLP.map(x => (x.features,x.label,mdlTree.predict(x.features)))

      //Predicting value
      val predictions = mdlTree.predict(testDataDDLP.map(x=>x.features))

      //Bundling predicted as well as actual values
      val labelsAndPreds = testDataDDLP.map(x=>x.label).zip(predictions)


      //Putting actual and predicted values in a matrix
      val metrics = new MulticlassMetrics(labelsAndPreds)


      //Calculating Mean Squared Error
      val mse = labelsAndPreds.map( vp => math.pow( (vp._1 - vp._2),2 ) ).reduce(_+_) / labelsAndPreds.count()


      //Calculating accuracy by dividing correctValues by totalNumber of values
      val correctVals = labelsAndPreds.aggregate(0.0)((x, rec) => x+ (rec._1 == rec._2).compare(false), _ + _)
      val accuracy = correctVals/labelsAndPreds.count()

      println("Tree Details: "+mdlTree)
      println("Tree Depth: "+mdlTree.depth)
      println("Learned classification tree model:\n" + mdlTree.toDebugString)
      println("Confusion Matrix:\n"+metrics.confusionMatrix)
      println("Mean Squared Error = " + "%6f".format(mse))
      println("Accuracy = " + "%3.2f%%".format(accuracy*100))
      println("Saving data to given path:::"+outputData_file_path)
      output_prediction.coalesce(1).saveAsTextFile(outputData_file_path)
      println("Saved Data in Format---->\n [Feature columns],actual value,predicted value")
      println("*** Done ***")


    }
  }


