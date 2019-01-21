package hr.fer.rassus.flink_consumer.flink.functions.aggregate

trait AggregationTypeGetter {
  def getAggregationType(): String
}
