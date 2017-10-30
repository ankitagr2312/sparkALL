


object fact {
  def main(args: Array[String]): Unit = {
    val someList = List(0,1,2,3,4,5,6)
    val (front,back) = someList.splitAt(1)

    front.foreach(x => println("front "+x))
    back.foreach(x => println("back "+x))

    var someOtherList = List(9,90,909)
    someOtherList = front

    someOtherList.foreach(println)

  }
  def factorial(num:Int): Int ={
    (1 to num).fold(1)((product,current)=> product * current)
  }
}
