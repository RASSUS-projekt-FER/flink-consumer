package hr.fer.rassus.flink_consumer

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object Job {
  def main(args: Array[String]) {
    // the port to connect to
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream("localhost", port, '\n')
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .sum(1)

    counts print

    env.execute("Scala SocketTextStreamWordCount Example")
  }
}
