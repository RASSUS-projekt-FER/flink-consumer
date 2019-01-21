package hr.fer.rassus.flink_consumer.functions.process.window

import hr.fer.rassus.flink_consumer.AggregatedMetric
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AggregatedMetricWrapFunction(val metricName: String, val aggregationType: String)
  extends ProcessWindowFunction[Double, AggregatedMetric, String, TimeWindow] {

  override def process(deviceName: String, context: Context, aggregates: Iterable[Double], out: Collector[AggregatedMetric]) {
    val aggregate = aggregates.iterator.next()
    out.collect(new AggregatedMetric(metricName, aggregationType, aggregate, deviceName))
  }
}
