/**
  * Created by tkmae6e on 03/04/17.
  */


import scala.util.parsing.combinator._

class Arith extends JavaTokenParsers {

  def outputGenerator(a:Any) :Unit = a match {
    case a:String => print(a)
    case ~(a,b) =>
      outputGenerator(a)
      outputGenerator(b)
    case lst:List[Any] =>
      lst foreach outputGenerator
  }

  def expr: Parser[Any] = term ~ rep(("+"|"-") ~ term)
  def term: Parser[Any] = factor ~ rep(("*"|"/") ~ factor)
  def factor: Parser[Any] = floatingPointNumber | "(" ~ expr ~ ")"

  def expr0: Parser[Double] = term0~rep(("+"|"-")~term0) ^^ {
    case t1~lst => lst.foldLeft(t1)((x,t) => if (t._1=="+") t1+t._2 else t1-t._2)
  }
  def term0: Parser[Double] = factor0~rep(("*"|"/")~factor0) ^^ {
    case t1~lst => lst.foldLeft(t1)((x,t) => if (t._1=="*") t1*t._2 else t1/t._2)
  }
  def factor0: Parser[Double] = floatingPointNumber ^^ (s => s.toDouble) | "("~> expr0 <~ ")" ^^ (f => f)
}

object Grammer extends Arith {
  def main(args: Array[String]) {
    println("input : "+ "10+2")
    println(parseAll(expr0,"(2+2)*5").get)
  }
}



//Extractor Json FInalization: getting source details, checkP{oint Directory,
//Source Analyzer
//Parser
//Error Validation
//DataFrame Construct
//Expression
//Join--> Constrotor
//Spark CheckPointing Code Example