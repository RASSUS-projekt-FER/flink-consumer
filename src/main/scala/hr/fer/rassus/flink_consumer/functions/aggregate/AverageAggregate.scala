package hr.fer.rassus.flink_consumer.functions.aggregate

import hr.fer.rassus.flink_consumer.KafkaMessage
import org.apache.flink.api.common.functions.AggregateFunction


case class AverageAccumulator(var sum: Double = 0.0, var cnt: Int = 0)

class AverageAggregate extends AggregateFunction[KafkaMessage, AverageAccumulator, Double]{
  override def createAccumulator() = new AverageAccumulator

  override def add(metric: KafkaMessage, accumulator: AverageAccumulator) = {
    accumulator.sum += metric.value
    accumulator.cnt += 1
    accumulator
  }

  override def getResult(accumulator: AverageAccumulator) = accumulator.sum / accumulator.cnt

  override def merge(a: AverageAccumulator, b: AverageAccumulator) =
    new AverageAccumulator(a.sum + b.sum, a.cnt + b.cnt)
}

object AverageAggregate extends AggregationTypeGetter {
  def getAggregationType(): String = "avg"
  def create(): AverageAggregate = new AverageAggregate
}