package hr.fer.rassus.flink_consumer

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import hr.fer.rassus.flink_consumer.flink.functions.aggregate._
import hr.fer.rassus.flink_consumer.flink.functions.process.window.AggregatedMetricWrapFunction
import hr.fer.rassus.flink_consumer.flink.serialization.MetricDeserializationSchema

object Job extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")

  val topicName: String = "cpu"  // can also configure list of topics

  val topicNames = Vector("cpu", "ram", "tcp-sent")
  println("hello2")

  val deserializationSchema = new MetricDeserializationSchema(topicName)

  val cpuUsages = env.addSource(new FlinkKafkaConsumer[Metric](topicName, deserializationSchema, properties))

  val windowedStream = cpuUsages
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
    .map{ msg => new AggregatedMetric(msg.deviceName, topicName, "SUM", msg.value)}

  val minStream = windowedStream.minBy("value")
    .map{ msg => new AggregatedMetric(msg.deviceName, topicName, "MIN", msg.value)}

  val maxStream = windowedStream.maxBy("value")
    .map{ msg => new AggregatedMetric(msg.deviceName, topicName, "MAX", msg.value)}

  averageStream.print()
  p99Stream.print()

  env.execute("Scala Rassus Flink Consumer")

}
