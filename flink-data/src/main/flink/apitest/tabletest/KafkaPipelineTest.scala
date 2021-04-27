package apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafkaPipelineTest {
  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    //2.从kafka读取数据
    tableEnv.connect( new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("zookeeper.connect","localhost:2181")
      .property("bootstrap.servers","localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    //3.查询转换
    val sensorTable = tableEnv.from("kafkaInputTable")
    //3.1 简单转换
    val resultTable = sensorTable
      .select('id,'temp)
      .filter('id === "sensor_1")
    //3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id) //基于ID分组
      .select('id,'id.count as 'count)

    //4.输出到kafka
    tableEnv.connect( new Kafka()
      .version("0.11")
      .topic("sinktest")
      .property("zookeeper.connect","localhost:2181")
      .property("bootstrap.servers","localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    resultTable.insertInto("kafkaOutputTable")


    env.execute("Kafka Pipeline Test")
  }
  case class SensorReading(id: String, timestamp: Long, temperature: Double)
}
