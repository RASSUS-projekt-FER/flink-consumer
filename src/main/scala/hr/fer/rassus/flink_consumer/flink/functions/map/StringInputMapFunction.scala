package hr.fer.rassus.flink_consumer.flink.functions.map

import hr.fer.rassus.flink_consumer.KafkaMessage
import hr.fer.rassus.flink_consumer.ModelJsonProtocol._
import org.apache.flink.api.common.functions.MapFunction
import spray.json._

class StringInputMapFunction extends MapFunction[String, KafkaMessage] {
  override def map(value: String): KafkaMessage = value.parseJson.convertTo[KafkaMessage]
}

