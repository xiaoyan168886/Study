package flink.analysis.networkflow


import java.net.URL
import java.sql.Timestamp
import java.util
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import scala.collection.mutable.ListBuffer

case class LogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

case class UrlViewCount(url:String,windowEnd:Long,count:Long)

object NetworkFlow {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val url:URL = getClass.getResource("/apache.log")
    val textFileStream: DataStream[String] = env.readTextFile(url.getPath)
    val splitStream: DataStream[LogEvent] = textFileStream.map(logData => {
      val logDataArray: Array[String] = logData.split(" ")
      val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val eventTime: Long = format.parse(logDataArray(3).trim).getTime
      LogEvent(logDataArray(0).trim, logDataArray(1).trim, eventTime, logDataArray(5).trim, logDataArray(6).trim)
    })
    val resultStream: DataStream[String] = splitStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogEvent](Time.seconds(1)) {
      override def extractTimestamp(element: LogEvent): Long = element.eventTime
    })
        .filter(data =>{
          val pattern = "^((?!\\.(css|js)$).)*$".r
          (pattern findFirstIn(data.url)).nonEmpty
        })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrl(5))
    resultStream.print()
    env.execute("log data process job")
  }

}

class CountAgg extends AggregateFunction[LogEvent,Long,Long]{
  override def add(value: LogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


class WindowResult extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}

class TopNHotUrl(topSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  lazy val urlState:ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState",classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
    val urlStateIter: util.Iterator[UrlViewCount] = urlState.get().iterator()
    while(urlStateIter.hasNext){
      allUrlViews += urlStateIter.next()
    }
    urlState.clear()

    val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortWith(_.count > _.count).take(topSize)

    val result: StringBuilder = new StringBuilder()
    result.append("window time: ").append(new Timestamp(timestamp - 1)).append("\n")

    for(i <- sortedUrlViews.indices){
      val currentUrlView = sortedUrlViews(i)
      result.append("NO").append(i + 1).append(" url=").append(currentUrlView.url).append(" count=").append(currentUrlView.count).append("\n")
    }
   result.append("**************************************")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}