package flinkdemo

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat

import flinkdemo.SocketTextStreamWordCount.WordWithCount
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

object WordCountSinkToMySQL {

  // *****************************************************************************
  // open()中只执行一次，开始时执行；invoke根据input进行sql执行；close()最后时关闭
  // *****************************************************************************
  class WordCountSinkToMySQL extends RichSinkFunction[WordWithCount] {

    var conn: Connection = null
    var ps: PreparedStatement = null

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    override def open(parameters: Configuration): Unit = {
      Class.forName("com.mysql.jdbc.Driver")

      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/mytest", "root", "mlm@2019")
      val sql = "insert into word_count(word,count,dt) values (?,?,?)"
      ps = conn.prepareStatement(sql)
    }


    override def invoke(in: WordWithCount): Unit = {
      try {
        ps.setString(1, in.word) //时间
        ps.setLong(2, in.count) //分钟
        ps.setString(3, format.format(new java.util.Date)) //窗口开始范围
        ps.executeUpdate()

      } catch {
        case e: Exception => println(e.getMessage)
      }
    }

    override def close(): Unit = {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }

  }

}
