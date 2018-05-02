package com.cx

/**
  * @author xi.cheng
  */
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {

  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]) : Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //nc -l 8082
    val text = env.socketTextStream("localhost", 8082, '\n')
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")
    windowCounts.print().setParallelism(1)
    env.execute("Socket Window WordCount")
  }
}