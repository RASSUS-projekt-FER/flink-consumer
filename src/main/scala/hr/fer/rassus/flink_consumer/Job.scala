package hr.fer.rassus.flink_consumer

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import spray.json._
import hr.fer.rassus.flink_consumer.flink.functions.aggregate._
import hr.fer.rassus.flink_consumer.flink.functions.process.window.AggregatedMetricWrapFunction

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

  val metric_name: String = "cpuUsage"
  //kafka topic about cpuUsage metrics
  val cpuUsages = env.socketTextStream("localhost", port, '\n')

  val windowedStream = cpuUsages.map { _.parseJson.convertTo[KafkaMessage] }
    .keyBy(_.deviceName)
    .timeWindow(Time.seconds(60))

  val averageStream = windowedStream.aggregate(
    AverageAggregate.create,
    new AggregatedMetricWrapFunction(metric_name, AverageAggregate.getAggregationType())
  )

  val p99Stream = windowedStream.aggregate(
    P99Aggregate.create,
    new AggregatedMetricWrapFunction(metric_name, P99Aggregate.getAggregationType())
  )

  val sumStream = windowedStream.sum("value")
    .map{ msg => new AggregatedMetric(metric_name, "SUM", msg.value, msg.deviceName)}

  val minStream = windowedStream.minBy("value")
    .map{ msg => new AggregatedMetric(metric_name, "MIN", msg.value, msg.deviceName)}

  val maxStream = windowedStream.maxBy("value")
    .map{ msg => new AggregatedMetric(metric_name, "MAX", msg.value, msg.deviceName)}

  averageStream.print()
  p99Stream.print()

  env.execute("Scala Rassus Flink Consumer")

}
