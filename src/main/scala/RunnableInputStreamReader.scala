import java.io.{BufferedReader, InputStream, InputStreamReader}

/**
  * Created by tkmae6e on 19/09/16.
  */
class RunnableInputStreamReader(is: InputStream, name: String) extends Runnable {

  val reader = new BufferedReader(new InputStreamReader(is))

  def run() = {
    var line = reader.readLine();
    while (line != null) {
      System.out.println(line);
      line = reader.readLine();
    }
    reader.close();
  }

}
