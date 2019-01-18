package hr.fer.rassus.flink_consumer

import spray.json._

case class KafkaMessage(var value: Double, var deviceName: String)
case class AggregatedMetric(var metricName: String, var  aggregationType: String,
                            var value: Double, var deviceName: String)

trait ModelJsonProtocol extends DefaultJsonProtocol {
  implicit val kafkaMessageFormat = jsonFormat2(KafkaMessage)
  implicit val aggregatedMetricFormat = jsonFormat4(AggregatedMetric)
}
