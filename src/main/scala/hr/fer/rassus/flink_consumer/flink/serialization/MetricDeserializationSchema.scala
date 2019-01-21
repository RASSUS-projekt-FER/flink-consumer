package hr.fer.rassus.flink_consumer.flink.serialization

import spray.json._
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import hr.fer.rassus.flink_consumer.Metric
import hr.fer.rassus.flink_consumer.ModelJsonProtocol._

import scala.util.Random

class MetricDeserializationSchema(val metricName: String) extends AbstractDeserializationSchema[Metric] {

  val random: Random = new Random

  override def deserialize(message: Array[Byte]): Metric = {
    try {
      val string = new String(message, "UTF-8")
      // val metric = string.toJson.convertTo[Metric]
      print(string)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    Metric(random.nextDouble() * 100, "a")
  }

  override def isEndOfStream(nextElement: Metric): Boolean = false

}
