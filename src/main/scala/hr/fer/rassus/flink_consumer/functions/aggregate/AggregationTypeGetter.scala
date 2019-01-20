package hr.fer.rassus.flink_consumer.functions.aggregate

trait AggregationTypeGetter {
  def getAggregationType(): String
}
