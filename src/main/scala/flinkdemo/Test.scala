package flinkdemo

import java.text.SimpleDateFormat
import java.util.Date

object Test {
  def main(args: Array[String]): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val dt = (new Date).getTime + 20000;
    System.out.println(format.format(dt))
    System.out.println(dt + ",boos1,pc1,200.0")
  }
}
