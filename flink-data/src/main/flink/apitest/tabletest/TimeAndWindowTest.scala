package apitest.tabletest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.types.Row

object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,settings)

    //读取数据
    val inputPath = "src/main/resources/1.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)
    //转换为样例类
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading)=t.timestamp * 1000L
      })

    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime)

    //1. Group Window
    //1.table api
    val resultTable: Table = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw) // 每10秒统计一次,滚动时间窗口
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'temperature.avg, 'tw.end)

    //2. sql
    tableEnv.createTemporaryView("sensor",sensorTable)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select
        | id,
        | count(id),
        | avg(temperature),
        | tumble_end(ts,interval '10' second)
        |from sensor
        |group by
        | id,
        | tumble(ts,interval '10' second)
        |""".stripMargin
    )

    //转换成流打印输出
    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toRetractStream[Row].print("Sql")

//    sensorTable.printSchema()
//    sensorTable.toAppendStream[Row].print()


    env.execute("Time And Window Test")
  }
  case class SensorReading(id: String, timestamp: Long, temperature: Double)
}
