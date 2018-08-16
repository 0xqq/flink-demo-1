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
      .path("D:\\IdeaProjects\\flink-demo\\LoanStats3a.csv")
      .field("installment", Types.DOUBLE)
      .field("grade", Types.STRING)
      .field("home_ownership", Types.STRING)
      .field("loan_status", Types.STRING)
      .fieldDelimiter(",")
      .lineDelimiter("\n")
      .ignoreFirstLine
      .ignoreParseErrors
      .commentPrefix("%")
      .build

    tableEnv.registerTableSource("LoanStats3a", csvTableSource)

    //val result = tableEnv.sql("SELECT home_ownership,loan_status, SUM(installment) AS installment_sum FROM LoanStats3a WHERE loan_status in ('Charged Off','Fully Paid') GROUP BY home_ownership,loan_status")

    val table = tableEnv.scan("LoanStats3a")
    val result = table
      .filter("loan_status='Charged Off' || loan_status='Fully Paid'")
      .groupBy("home_ownership,loan_status")
      .select("home_ownership,loan_status,installment.sum AS installment_sum");

    //创建一个 TableSink
    val sink = new CsvTableSink("D:\\IdeaProjects\\flink-demo\\LoanStats3a_output.csv", fieldDelim = ",", numFiles = 1, writeMode = WriteMode.OVERWRITE)

    // 将结果 Table写入TableSink中
    result.writeToSink(sink)

    env.setParallelism(1)
    env.execute("CsvTableTest")
  }
}
