package hr.fer.rassus.flink_consumer

import spray.json._

case class Device(var id: String, var name: String)
case class Metric(var name: String, var value: Int, device: Device)

trait ModelJsonProtocol extends DefaultJsonProtocol {
  implicit val deviceFormat = jsonFormat2(Device)
  implicit val metricFormat = jsonFormat3(Metric)
}
