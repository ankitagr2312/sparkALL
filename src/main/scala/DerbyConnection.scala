import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import org.apache.derby._
import java.net.InetAddress
import java.sql.DriverManager



object DerbyConnection {
  def main(args: Array[String]): Unit = {
val dbURL = "jdbc:derby://localhost:1527/myDB;create=true;"
val tableName = "kafka"

    Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
    //Get a connection
    val conn = DriverManager.getConnection(dbURL);

    val stm = conn.createStatement()

    val result = stm.executeQuery("select * from app.kafka")

    while (result.next())
      {
        println(result.getString(2))
      }
  }
}
