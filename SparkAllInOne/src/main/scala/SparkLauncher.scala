/**
  * Created by tkmadks on 9/14/16.
  */

import org.apache.spark.launcher.SparkLauncher;
object SparkLauncher extends SparkLauncher{
  def main(args: Array[String]) {

    val sparkLauncher = launcherInstance

    sparkLauncher.setSparkHome("/usr/hdp/2.4.0.0-169/spark")
      .setAppResource("/root/Documents/SparkLauncher/target/scala-2.10/sparkallinone_2.10-1.0.jar")
      .setMainClass("SampleStreamExample")
      .setMaster("yarn")
      .setDeployMode("cluster")
    val sparkLauncher1 = sparkLauncher.startApplication()

    var str = sparkLauncher1.getState().toString
    val jobAppId = sparkLauncher1.getAppId
    var jobStatus = sparkLauncher1.getState.toString
    while (true) {
      //if (!sparkLauncher1.getState().toString.equals(jobStatus)) {
        println(sparkLauncher1.getState().toString)
        //jobStatus = sparkLauncher1.getState().toString()
      //}
    }
  }
  def launcherInstance(): SparkLauncher = {
    new SparkLauncher
  }
}
