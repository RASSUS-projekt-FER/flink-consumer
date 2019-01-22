package hr.fer.rassus.flink_consumer.flink.functions.map

import hr.fer.rassus.flink_consumer.{AggregatedMetric, FilteredControl, Control}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class FilterFunction extends RichCoFlatMapFunction[Control, AggregatedMetric, FilteredControl] {

  var configs = new mutable.ListBuffer[Control]()

  override def flatMap1(value: Control, out: Collector[FilteredControl]): Unit = configs.append(value)

  override def flatMap2(value: AggregatedMetric, out: Collector[FilteredControl]): Unit = {
    val eventConfigs = configs.filter{
      x => (x.deviceName == value.deviceName) || (x.metricName != value.metricName)
    }

    if (eventConfigs.size > 0) {
      out.collect(FilteredControl(value, eventConfigs.toList))
    }
  }
}
