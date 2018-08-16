package flinkdemo

import org.apache.flink.api.scala._

object WordCountTest {
  def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val data = List("hello", "flink cluster", "hello")
        val ds = env.fromCollection(data)
        val words = ds.flatMap(value => value.split("\\s+"))
        val mappedWords = words.map(value => (value, 1))
        val grouped = mappedWords.groupBy(0)
        val sum = grouped.sum(1)
        println(sum.collect())
  }
}
