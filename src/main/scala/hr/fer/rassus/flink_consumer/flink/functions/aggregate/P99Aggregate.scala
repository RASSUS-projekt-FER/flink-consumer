package hr.fer.rassus.flink_consumer.flink.functions.aggregate

import hr.fer.rassus.flink_consumer.Metric
import org.apache.flink.api.common.functions.AggregateFunction

import scala.collection.mutable.ArrayBuffer


case class P99Accumulator(buffer: ArrayBuffer[Double] = ArrayBuffer.empty[Double])

class P99Aggregate extends AggregateFunction[Metric, P99Accumulator, Double] {
  override def createAccumulator() = new P99Accumulator

  override def add(message: Metric, accumulator: P99Accumulator) = {
    accumulator.buffer += message.value
    accumulator
  }

  override def getResult(accumulator: P99Accumulator): Double = {
    val sorted: Array[Double] = accumulator.buffer.toArray.sorted
    val index = Math.ceil(0.99 * sorted.length).toInt - 1
    sorted(index)
  }

  override def merge(a: P99Accumulator, b: P99Accumulator) =
    new P99Accumulator(a.buffer ++ b.buffer)
}


object P99Aggregate extends AggregationTypeGetter {
  override def getAggregationType(): String = "P99"
  def create(): P99Aggregate = new P99Aggregate
}