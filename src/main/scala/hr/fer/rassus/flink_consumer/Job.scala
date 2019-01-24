package hr.fer.rassus.flink_consumer

import java.util.Properties

import hr.fer.rassus.flink_consumer.flink.functions.aggregate._
import hr.fer.rassus.flink_consumer.flink.functions.map.{FilterFunction, QualifierFunction}
import hr.fer.rassus.flink_consumer.flink.functions.sink.MetricDashboardSink
import hr.fer.rassus.flink_consumer.flink.functions.window.AggregatedMetricWrapFunction
import hr.fer.rassus.flink_consumer.flink.selectors.DeviceMetricKeySelector
import hr.fer.rassus.flink_consumer.flink.serialization.{ControlSchema, MetricSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Job extends App {

  def aggregateMetrics(metrics: DataStream[Metric]): DataStream[AggregatedMetric] = {
    val keyedStream = metrics.keyBy(new DeviceMetricKeySelector)
      .timeWindow(Time.seconds(10))

    val averageStream = keyedStream.aggregate(
      new AverageAggregate,
      new AggregatedMetricWrapFunction("AVERAGE")
    ).name("Average")

    val p99Stream = keyedStream.aggregate(
      new P99Aggregate,
      new AggregatedMetricWrapFunction("P99")
    ).name("P99")

    val sumStream = keyedStream.sum("value").name("SumBy")
      .map{ msg => new AggregatedMetric(msg.deviceName, msg.metricName, "SUM", msg.value)}

    val minStream = keyedStream.minBy("value").name("MinBy")
      .map{ msg => new AggregatedMetric(msg.deviceName, msg.metricName, "MIN", msg.value)}

    val maxStream = keyedStream.maxBy("value").name("MaxBy")
      .map{ msg => new AggregatedMetric(msg.deviceName, msg.metricName, "MAX", msg.value)}

    averageStream.union(p99Stream)
      .union(sumStream)
      .union(minStream)
      .union(maxStream)
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")

  val metricsTopic = "metrics-topic"
  val controlsTopic = "controls-topic"

  val metrics = env.addSource(new FlinkKafkaConsumer[Metric](metricsTopic, new MetricSchema, properties))
    .name("Kafka Metric Stream")
  val controls = env.addSource(new FlinkKafkaConsumer[Control](controlsTopic, new ControlSchema, properties))
    .name("Kafka Control Stream")

  val aggregatedMetrics = aggregateMetrics(metrics)

  controls.connect(aggregatedMetrics)
      .flatMap(new FilterFunction)
      .name("Filter Function")
      .flatMap(new QualifierFunction)
      .name("Qualifier Function")
      .print()

  aggregatedMetrics.addSink(new MetricDashboardSink)
      .name("Metric Dashboard Sink")

  env.execute("Scala Rassus Flink Consumer")
}

