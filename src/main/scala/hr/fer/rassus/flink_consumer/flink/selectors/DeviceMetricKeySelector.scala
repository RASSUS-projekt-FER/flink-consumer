package hr.fer.rassus.flink_consumer.flink.selectors

import hr.fer.rassus.flink_consumer.Metric
import org.apache.flink.api.java.functions.KeySelector

class DeviceMetricKeySelector extends KeySelector[Metric, (String, String)]{
  override def getKey(value: Metric): (String, String) =
    (value.deviceName, value.metricName)
}
