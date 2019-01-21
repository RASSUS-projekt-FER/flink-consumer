package hr.fer.rassus.flink_consumer.flink.serialization

import spray.json._
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import hr.fer.rassus.flink_consumer.Metric
import hr.fer.rassus.flink_consumer.ModelJsonProtocol._

class MetricDeserializationSchema(val metricName: String) extends AbstractDeserializationSchema[Metric] {
  override def deserialize(message: Array[Byte]): Metric = {
    try {
      val string = new String(message, "UTF-8")
      // val metric = string.toJson.convertTo[Metric]
      print((string).mkString)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    Metric(12.2, "a")
  }

  override def isEndOfStream(nextElement: Metric): Boolean = false

}
