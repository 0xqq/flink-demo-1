package flinkdemo

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 实验目的:
  * 1.以operator的系统时间为准划分窗口，计算5秒钟内出价最高的信息。
  * 2.因为是以实际的operator的systemTime为标准，那么消息中可以没有睡觉属性。
  * 3.flink默认的就是这种时间窗口，以前我们就是使用的这种窗口
  * 测试数据(消息中的时间属性不必须，有也不会使用)
  * boos1,pc1,100.0
  * boos2,pc1,200.0
  * boos1,pc1,300.0
  * boos2,pc2,500.0
  * boos2,pc2,600.0
  * boos2,pc2,700.0
  */
object ProcessingTimeExample {
  def main(args: Array[String]) {

    //1.创建执行环境，并设置为使用ProcessingTime
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置为使用ProcessingTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //2.创建数据流，并进行数据转化
    val source = env.socketTextStream("localhost", 9999)
    case class SalePrice(boosName: String, productName: String, price: Double)

    val dst1: DataStream[SalePrice] = source.map(value => {
      val columns = value.split(",")
      SalePrice(columns(0), columns(1), columns(2).toDouble)
    })

    //3.使用ProcessingTime进行求最值操作,不需要提取消息中的时间属性
    val dst2: DataStream[SalePrice] = dst1
      .keyBy(_.productName)
      .timeWindow(Time.seconds(5))//设置window方法一
      //.window(TumblingEventTimeWindows.of(Time.seconds(5))) //设置window方法二
      .max("price")

    //4.显示结果
    dst2.print()

    //5.触发流计算
    env.execute("ProcessingTime example")
  }


}
