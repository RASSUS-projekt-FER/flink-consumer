package hr.fer.rassus.flink_consumer

import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import spray.json._
import hr.fer.rassus.flink_consumer.flink.functions.aggregate._
import hr.fer.rassus.flink_consumer.flink.functions.process.window.AggregatedMetricWrapFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object Job extends App with ModelJsonProtocol {

  val port: Int = try {
    ParameterTool.fromArgs(args).getInt("port")
  } catch {
    case e: Exception => {
      System.err.println("No port specified. Please add '--port <port>'")
      sys.exit(1)
    }
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")
  properties.setProperty("group.id", "test-consumer-group")

  val topicName: String = "cpuTestiranje"  // can also configure list of topics
  val deserializationSchema = new SimpleStringSchema() // to_do: change to keyeddeserializationschmea

  val cpuUsages = env.addSource(new FlinkKafkaConsumer[String](topicName, deserializationSchema, properties))

  val windowedStream = cpuUsages.map { _.parseJson.convertTo[KafkaMessage] }
    .keyBy(_.deviceName)
    .timeWindow(Time.seconds(60))

  val averageStream = windowedStream.aggregate(
    AverageAggregate.create,
    new AggregatedMetricWrapFunction(topicName, AverageAggregate.getAggregationType())
  )

  val p99Stream = windowedStream.aggregate(
    P99Aggregate.create,
    new AggregatedMetricWrapFunction(topicName, P99Aggregate.getAggregationType())
  )

  val sumStream = windowedStream.sum("value")
    .map{ msg => new AggregatedMetric(topicName, "SUM", msg.value, msg.deviceName)}

  val minStream = windowedStream.minBy("value")
    .map{ msg => new AggregatedMetric(topicName, "MIN", msg.value, msg.deviceName)}

  val maxStream = windowedStream.maxBy("value")
    .map{ msg => new AggregatedMetric(topicName, "MAX", msg.value, msg.deviceName)}

  averageStream.print()
  p99Stream.print()

  env.execute("Scala Rassus Flink Consumer")

}
