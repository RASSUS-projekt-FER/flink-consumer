package hr.fer.rassus.flink_consumer.flink.functions.window

import hr.fer.rassus.flink_consumer.AggregatedMetric
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AggregatedMetricWrapFunction(val aggregationType: String)
  extends ProcessWindowFunction[Double, AggregatedMetric, (String, String), TimeWindow] {

  override def process(deviceMetricKey: (String, String), context: Context,
                       aggregates: Iterable[Double], out: Collector[AggregatedMetric]) {
    val aggregate = aggregates.iterator.next()
    out.collect(new AggregatedMetric(deviceMetricKey._1, deviceMetricKey._2, aggregationType, aggregate))
  }
}
