package hr.fer.rassus.flink_consumer.flink.serialization

import hr.fer.rassus.flink_consumer.Metric
import hr.fer.rassus.flink_consumer.ModelJsonProtocol._
import spray.json._
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

class MetricSchema extends AbstractDeserializationSchema[Metric] {

  override def deserialize(message: Array[Byte]): Metric = {
      val string = new String(message, "UTF-8")
      println("Received metric message: " + string)
      string.parseJson.convertTo[Metric]
  }

  override def isEndOfStream(nextElement: Metric): Boolean = false
}
