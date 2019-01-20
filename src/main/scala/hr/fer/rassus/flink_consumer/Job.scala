package hr.fer.rassus.flink_consumer

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import hr.fer.rassus.flink_consumer.functions.aggregate._
import hr.fer.rassus.flink_consumer.functions.process.window.AggregatedMetricWrapFunction
import hr.fer.rassus.flink_consumer.functions.sink.MetricDashboardSink
import hr.fer.rassus.flink_consumer.functions.map.StringInputMapFunction

object Job extends App {

  val port: Int = try {
    ParameterTool.fromArgs(args).getInt("port")
  } catch {
    case _:  Exception =>
      System.err.println("No port specified. Please add '--port <port>'")
      sys.exit(1)
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val metric_name: String = "cpuUsage"
  //kafka topic about cpuUsage metrics


  val cpuUsages = env.socketTextStream("localhost", port, '\n')

  val windowedStream = cpuUsages.map(new StringInputMapFunction)
    .keyBy(_.deviceName)
    .timeWindow(Time.seconds(60))

  val averageStream = windowedStream.aggregate(
    AverageAggregate.create(),
    new AggregatedMetricWrapFunction(metric_name, AverageAggregate.getAggregationType())
  )

  val p99Stream = windowedStream.aggregate(
    P99Aggregate.create(),
    new AggregatedMetricWrapFunction(metric_name, P99Aggregate.getAggregationType())
  )

  val sumStream = windowedStream.sum("value")
    .map{ msg => AggregatedMetric(metric_name, "sum", msg.value, msg.deviceName)}

  val minStream = windowedStream.minBy("value")
    .map{ msg => AggregatedMetric(metric_name, "min", msg.value, msg.deviceName)}

  val maxStream = windowedStream.maxBy("value")
    .map{ msg => AggregatedMetric(metric_name, "max", msg.value, msg.deviceName)}

  averageStream.addSink(new MetricDashboardSink)

  env.execute("Scala Rassus Flink Consumer")
}
