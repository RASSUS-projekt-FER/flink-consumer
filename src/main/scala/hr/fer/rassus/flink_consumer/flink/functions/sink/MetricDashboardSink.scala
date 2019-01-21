package hr.fer.rassus.flink_consumer.flink.functions.sink

import hr.fer.rassus.flink_consumer.ModelJsonProtocol._
import hr.fer.rassus.flink_consumer.{AggregatedMetric, AggregatedMetricObservation}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import scalaj.http.Http
import spray.json._

class MetricDashboardSink extends RichSinkFunction[AggregatedMetric] {

  override def invoke(metric: AggregatedMetric) = {
    val deviceName = metric.deviceName
    val metricName = metric.metricName
    val aggregationName = metric.aggregationType
    val url = s"http://localhost:5000/devices/$deviceName/metrics/$metricName/aggregations/$aggregationName"

    val aggregatedMetricObservation = AggregatedMetricObservation(metric.value)

    Http(url).postData(aggregatedMetricObservation.toJson.compactPrint)
    .header("Content-Type", "application/json")
    .header("Charset", "UTF-8")
    .execute()
  }
}