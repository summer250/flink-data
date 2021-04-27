package apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object tableapi {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    /*
        // 1.1 基于老版本planner的流处理
        val settings = EnvironmentSettings.newInstance()
          .useOldPlanner()
          .inStreamingMode()
          .build()
        val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

        // 1.2 基于老版本的批处理
        val batchEnv = ExecutionEnvironment.getExecutionEnvironment
        val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

        // 1.3 基于blink planner的流处理
        val blinkStreamSettings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build()
        val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

        // 1.4 基于blink planner的批处理
        val blinkBatchSettings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inBatchMode()
          .build()
        val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)
        */

    //1.连接外部系统,读取数据,注册表
    //2.1 读取文件
    val filePath = "src/main/resources/1.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat( new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")
//
//    val inputTable: Table = tableEnv.from("inputTable")
//    inputTable.toAppendStream[(String,Long,Double)].print()

    //2.2 从kafka读取数据
//    tableEnv.connect( new Kafka()
//      .version("0.11")
//      .topic("sensor")
//      .property("zookeeper.connect","localhost:2181")
//      .property("bootstrap.servers","localhost:9092")
//    )
//      .withFormat(new Csv())
//      .withSchema(new Schema()
//        .field("id",DataTypes.STRING())
//        .field("timestamp",DataTypes.BIGINT())
//        .field("temperature",DataTypes.DOUBLE())
//      )
//      .createTemporaryTable("kafkaInputTable")
//
//      val inputTable: Table = tableEnv.from("kafkaInputTable")
//      inputTable.toAppendStream[(String,Long,Double)].print()


    //3.查询转换
    //3.1 使用table api
//    val sensorTable = tableEnv.from("inputTable")
//    val resultTable = sensorTable
//      .select("id,temperature")
//      .filter(id === "sensor_1")

    //3.2 SQL
//    val resultSqlTable = tableEnv.sqlQuery(
//      """
//        |select id,temperature
//        |from inputTable
//        |where id='sensor_1'
//        |""".stripMargin)
//
//    resultSqlTable.toAppendStream[(String,Double)].print("sql")

    env.execute("table api test")

  }
  case class SensorReading(id: String, timestamp: Long, temperature: Double)
}
