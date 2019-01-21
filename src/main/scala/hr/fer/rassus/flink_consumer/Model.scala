package hr.fer.rassus.flink_consumer

import spray.json._

case class Metric(value: Double, deviceName: String)
case class AggregatedMetric(deviceName: String, metricName: String, aggregationType: String, value: Double)

object ModelJsonProtocol extends DefaultJsonProtocol {
  implicit val kafkaMessageFormat = jsonFormat2(Metric)
  implicit val aggregatedMetricFormat = jsonFormat4(AggregatedMetric)
}
