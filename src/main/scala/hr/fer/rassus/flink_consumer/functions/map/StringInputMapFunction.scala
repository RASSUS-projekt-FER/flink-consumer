package hr.fer.rassus.flink_consumer.functions.map

import org.apache.flink.api.common.functions.MapFunction
import spray.json._
import hr.fer.rassus.flink_consumer.KafkaMessage
import hr.fer.rassus.flink_consumer.ModelJsonProtocol._

class StringInputMapFunction extends MapFunction[String, KafkaMessage] {
  override def map(value: String): KafkaMessage = value.parseJson.convertTo[KafkaMessage]
}

