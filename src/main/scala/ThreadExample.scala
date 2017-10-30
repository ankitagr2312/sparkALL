/**
  * Created by tkmae6e on 11/05/17.
  */

case class ThreadExample(whatIAmDoing: Int) extends Runnable {
  def run {
    val whatIAmDoing1 = whatIAmDoing + " is my name."
    println("* "+whatIAmDoing1+" *")
  }
}
object ThreadExample {
  var whatIAmDoing = 1
  def main(args: Array[String]): Unit = {
    new Thread(new ThreadExample(whatIAmDoing)).start
    whatIAmDoing +=1
    new Thread(new ThreadExample(whatIAmDoing)).start
    whatIAmDoing +=1
    new Thread(new ThreadExample(whatIAmDoing)).start
    whatIAmDoing +=1
    new Thread(new ThreadExample(whatIAmDoing)).start
    whatIAmDoing +=1
    new Thread(new ThreadExample(whatIAmDoing)).start
    whatIAmDoing +=1
    new Thread(new ThreadExample(whatIAmDoing)).start
    whatIAmDoing +=1
    new Thread(new ThreadExample(whatIAmDoing)).start

  }
}
