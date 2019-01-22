package hr.fer.rassus.flink_consumer

import spray.json._

case class Metric(deviceName: String, metricName: String, value: Double)
case class AggregatedMetric(deviceName: String, metricName: String, aggregationType: String, value: Double)
case class AggregatedMetricObservation(value: Double)

case class Control(deviceName: String, metricName: String, aggregationType: String,
                   operator: String, threshold: Double)
case class FilteredControl(metric: AggregatedMetric, controls: List[Control])
case class QualifiedControl(metric: AggregatedMetric, control: Control)

object ModelJsonProtocol extends DefaultJsonProtocol {
  implicit val metricFormat = jsonFormat3(Metric)
  implicit val aggregatedMetricFormat = jsonFormat4(AggregatedMetric)
  implicit val aggregatedMetricObservationFormat = jsonFormat1(AggregatedMetricObservation)

  implicit val controlFormat = jsonFormat5(Control)
  implicit val qualifiedControlFormat = jsonFormat2(QualifiedControl)
}

