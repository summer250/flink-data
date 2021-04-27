package apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FileOutPutTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    val filePath = "src/main/resources/1.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat( new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temp",DataTypes.DOUBLE())
//        .field("pt",DataTypes.TIMESTAMP(3)).proctime()
      )
      .createTemporaryTable("inputTable")

    //3.转换操作
    val sensorTable = tableEnv.from("inputTable")

    //3.1 简单转换
    val resultTable = sensorTable
      .select('id,'temp)
      .filter('id === "sensor_1")

    //3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id) //基于ID分组
      .select('id,'id.count as 'count)

    //4.输出到文件
    //注册输出表
    val outputPath = "src/main/resources/output.txt"
    tableEnv.connect(new FileSystem().path(outputPath))
      .withFormat( new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("cnt",DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")

    aggTable.insertInto("outputTable")


//    resultTable.toAppendStream[(String,Double)].print("result")
//    aggTable.toRetractStream[(String,Long)].print("agg")

    env.execute("table api test")
  }
  case class SensorReading(id: String, timestamp: Long, temperature: Double)
}
