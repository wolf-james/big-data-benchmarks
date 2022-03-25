// Simple client
import java.net._
import scala.io.BufferedSource


object Client extends App {
  val s = new Socket(InetAddress.getByName("localhost"), 9999)
  lazy val in = new BufferedSource(s.getInputStream).getLines()
  in.foreach(println)

  s.close()
}
