import java.io.{BufferedReader, Closeable, IOException, InputStreamReader, PrintWriter}
import java.io.{BufferedOutputStream, BufferedReader}
import java.net.{InetAddress, ServerSocket, SocketException, SocketTimeoutException, Socket => JSocket}

import scala.io.Codec
import scala.reflect.io.Streamable

/** A skeletal only-as-much-as-I-need Socket wrapper.
  */
object EchoServer {
  def main(args: Array[String]): Unit = {
    val someVal = Seq("jjnj").mkString(",")
    /*val vg = someVal: _**/

  }
}