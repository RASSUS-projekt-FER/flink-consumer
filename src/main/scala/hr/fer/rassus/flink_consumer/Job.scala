package hr.fer.rassus.flink_consumer

import java.util.Properties

import hr.fer.rassus.flink_consumer.flink.functions.aggregate._
import hr.fer.rassus.flink_consumer.flink.functions.process.window.AggregatedMetricWrapFunction
import hr.fer.rassus.flink_consumer.flink.functions.sink.MetricDashboardSink
import hr.fer.rassus.flink_consumer.flink.serialization.MetricDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Job extends App {

  def createMetricAggregationStreams(metricName: String, streamExecutionEnvironment: StreamExecutionEnvironment,
                                     properties: Properties): Vector[DataStream[AggregatedMetric]] = {

    val deserializationSchema = new MetricDeserializationSchema(metricName)

    val metricStream = env.addSource(new FlinkKafkaConsumer[Metric](metricName, deserializationSchema, properties))
      .keyBy(_.deviceName)
      .timeWindow(Time.seconds(10))

    val averageStream = metricStream.aggregate(
      AverageAggregate.create,
      new AggregatedMetricWrapFunction(metricName, AverageAggregate.getAggregationType())
    )

    val p99Stream = metricStream.aggregate(
      P99Aggregate.create,
      new AggregatedMetricWrapFunction(metricName, P99Aggregate.getAggregationType())
    )

    val sumStream = metricStream.sum("value")
      .map{ msg => new AggregatedMetric(msg.deviceName, metricName, "SUM", msg.value)}

    val minStream = metricStream.minBy("value")
      .map{ msg => new AggregatedMetric(msg.deviceName, metricName, "MIN", msg.value)}

    val maxStream = metricStream.maxBy("value")
      .map{ msg => new AggregatedMetric(msg.deviceName, metricName, "MAX", msg.value)}


    Vector(averageStream, p99Stream, sumStream, minStream, maxStream)
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // env.setParallelism(2)

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")

  val topicNames = Vector("cpu", "ram", "tcp-sent")
  topicNames.map(createMetricAggregationStreams(_, env, properties))
      .flatMap(_.toList)
      .reduceLeft(_.union(_))
      .addSink(new MetricDashboardSink)

  env.execute("Scala Rassus Flink Consumer")
}

