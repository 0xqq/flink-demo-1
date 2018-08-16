package flinkdemo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource

object CsvTableTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val csvTableSource = CsvTableSource
      .builder
      .path("D:\\IdeaProjects\\flink-demo\\pub_product_info.csv")
      .field("prod_cd", Types.STRING)
      .field("prod_nm", Types.STRING)
      .field("prod_dept_cd", Types.STRING)
      .field("prod_dept", Types.STRING)
      .field("maintain_dt", Types.STRING)
      .field("upd_dt", Types.STRING)
      .field("upd_tm", Types.STRING)
      .fieldDelimiter(",")
      .lineDelimiter("\n")
      .ignoreFirstLine
      .ignoreParseErrors
      .commentPrefix("%")
      .build

    tableEnv.registerTableSource("pub_product_info", csvTableSource)

    //val result = tableEnv.sql("SELECT prod_dept, COUNT(prod_nm) AS prod_count FROM pub_product_info GROUP BY prod_dept")

    val table = tableEnv.scan("pub_product_info")
    val result = table
      //.filter("prod_dept='产品一部'")
      .groupBy("prod_dept")
      .select("prod_dept, prod_nm.count AS prod_count");

    //创建一个 TableSink
    val sink = new CsvTableSink("D:\\IdeaProjects\\flink-demo\\pub_product_info_output.csv", fieldDelim = ",", numFiles = 1, writeMode = WriteMode.OVERWRITE)

    // 将结果 Table写入TableSink中
    result.writeToSink(sink)

    env.setParallelism(1)
    env.execute("CsvTableTest")
  }
}
