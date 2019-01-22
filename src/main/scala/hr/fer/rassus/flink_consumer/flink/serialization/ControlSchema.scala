package hr.fer.rassus.flink_consumer.flink.serialization

import hr.fer.rassus.flink_consumer.Control
import hr.fer.rassus.flink_consumer.ModelJsonProtocol._
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import spray.json._

class ControlSchema extends AbstractDeserializationSchema[Control] {

  override def deserialize(message: Array[Byte]): Control = {
    val string = new String(message, "UTF-8")
    println("Received control message: " + string)
    string.parseJson.convertTo[Control]
  }

  override def isEndOfStream(nextElement: Control): Boolean = false
}