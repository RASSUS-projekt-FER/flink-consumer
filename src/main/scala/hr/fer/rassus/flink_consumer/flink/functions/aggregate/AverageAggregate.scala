package hr.fer.rassus.flink_consumer.flink.functions.aggregate

import hr.fer.rassus.flink_consumer.Metric
import org.apache.flink.api.common.functions.AggregateFunction


case class AverageAccumulator(var sum: Double = 0.0, var cnt: Int = 0)

class AverageAggregate extends AggregateFunction[Metric, AverageAccumulator, Double]{
  override def createAccumulator() = new AverageAccumulator

  override def add(metric: Metric, accumulator: AverageAccumulator) = {
    accumulator.sum += metric.value
    accumulator.cnt += 1
    accumulator
  }

  override def getResult(accumulator: AverageAccumulator) = accumulator.sum / accumulator.cnt

  override def merge(a: AverageAccumulator, b: AverageAccumulator) =
    new AverageAccumulator(a.sum + b.sum, a.cnt + b.cnt)
}

object AverageAggregate extends AggregationTypeGetter {
  override def getAggregationType(): String = "AVERAGE"
  def create(): AverageAggregate = new AverageAggregate
}