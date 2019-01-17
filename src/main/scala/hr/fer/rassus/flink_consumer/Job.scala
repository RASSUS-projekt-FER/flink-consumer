package hr.fer.rassus.flink_consumer

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import spray.json._

object Job extends App
  with ModelJsonProtocol {
  
  val port: Int = try {
    ParameterTool.fromArgs(args).getInt("port")
  } catch {
    case e: Exception => {
      System.err.println("No port specified. Please add '--port <port>'")
      sys.exit(1)
    }
  }

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  //Create streams for names and ages by mapping the inputs to the corresponding objects
  val text = env.socketTextStream("localhost", port, '\n')
  val counts = text.map { _.parseJson.convertTo[Metric] }
    .keyBy("name")
    .timeWindow(Time.seconds(50))
    .sum("value")

  counts.print()

  env.execute("Scala Rassus Flink Consumer")

}
