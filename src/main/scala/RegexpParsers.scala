import antlr.Parser

/**
  * Created by tkmae6e on 06/04/17.
  */

object RegexpParsers {
 /* def classPrefix = "class" ~ ID ~ "(" ~ formals ~ ")"
  implicit def keyword(str: String) = new Parser[String] {
    def apply(s: Stream[Character]) = {
      val trunc = s take str.length
      lazy val errorMessage = "Expected '%s' got '%s'".format(str, trunc.mkString)

      if (trunc lengthCompare str.length != 0)
        Failure(errorMessage)
      else {
        val succ = trunc.zipWithIndex forall {
          case (c, i) => c == str(i)
        }

        if (succ) Success(str, s drop str.length)
        else Failure(errorMessage)
      }
    }
  }*/
}
