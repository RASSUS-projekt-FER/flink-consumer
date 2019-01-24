package hr.fer.rassus.flink_consumer.flink.functions.map

import hr.fer.rassus.flink_consumer.{FilteredControl, QualifiedControl}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class QualifierFunction extends FlatMapFunction[FilteredControl, QualifiedControl]{

  def ~=(x: Double, y: Double, precision: Double) = {
    if ((x - y).abs < precision) true else false
  }

  def qualify(operator: String, threshold: Double, value: Double): Boolean = {
    operator.toUpperCase match {
      case "EQ" => ~=(threshold, value, 0.0001)
      case "GT" => return value > threshold
      case "GTE" => ~=(threshold, value, 0.0001) || value > threshold
      case "LT" => return value < threshold
      case "LTE" => ~=(threshold, value, 0.0001) || value < threshold
    }
  }

  override def flatMap(value: FilteredControl, out: Collector[QualifiedControl]): Unit = {
    value.controls.foreach{ control =>
      val result = qualify(control.operator, control.threshold, value.metric.value)
      if (result) {
        out.collect(QualifiedControl(value.metric, control))
      }
    }
  }
}
