import akka.actor.{Actor, ActorSystem, Props}
import akka.actor.Actor.Receive

/**
  * Created by tkmae6e on 22/09/16.
  */


class testActorClass extends Actor {
  override def receive: Receive = {
    case s:String => println(s)
    case i:Int    => println("huh?")
  }
}
object testActor {
  def main(args: Array[String]): Unit = {
    val system= ActorSystem("SimpleAkkaSystem")
    val actor = system.actorOf(Props[testActorClass],"testActorClass")
    actor ! "ankit"

  }

}
