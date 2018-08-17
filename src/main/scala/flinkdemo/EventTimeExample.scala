package flinkdemo

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 实验目的:
  * 1.以EventTime划分窗口，计算5秒钟内出价最高的信息
  * 2.要求消息本身就应该携带EventTime
  * 3.时间对应关系如下
  * 2016-04-27 11:34:22  1461756862000
  * 2016-04-27 11:34:27  1461756867000
  * 2016-04-27 11:34:32  1461756872000
  * 测试数据：
  * 1461756862000,boos1,pc1,100.0
  * 1461756867000,boos2,pc1,200.0
  * 1461756872000,boos1,pc1,300.0
  * 1461756862000,boos2,pc2,500.0
  * 1461756867000,boos2,pc2,600.0
  * 1461756872000,boos2,pc2,700.0
  */
object EventTimeExample {
  def main(args: Array[String]) {

    //1.创建执行环境，并设置为使用EventTime
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //置为使用EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.创建数据流，并进行数据转化
    val source = env.socketTextStream("localhost", 9999)
    case class SalePrice(time: Long, boosName: String, productName: String, price: Double)

    val dst1: DataStream[SalePrice] = source.map(value => {
      val columns = value.split(",")
      SalePrice(columns(0).toLong, columns(1), columns(2), columns(3).toDouble)
    })

    val watermark = dst1.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[SalePrice] {

      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 10000L//最大允许的乱序时间是10s

      var a : Watermark = null

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        a
      }

      override def extractTimestamp(t: SalePrice, l: Long): Long = {
        val timestamp = t.time
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println("timestamp:" + t.productName +","+ t.time + "|" +format.format(t.time) +","+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ","+ a.toString)
        timestamp
      }
    })

    //3.使用EventTime进行求最值操作
    val dst2: DataStream[SalePrice] = watermark
      .keyBy(_.productName)
      //.timeWindow(Time.seconds(5))//设置window方法一
      .window(TumblingEventTimeWindows.of(Time.seconds(10))) //设置window方法二
      .max("price")

    //4.显示结果
    // print the results with a single thread, rather than in parallel
    dst2.print().setParallelism(1)

    //5.触发流计算
    env.execute("EventTime example")
  }
}
