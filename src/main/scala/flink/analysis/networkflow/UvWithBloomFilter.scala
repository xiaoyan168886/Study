package flink.analysis.networkflow


import java.lang
import java.net.URL

import flink.analysis.hotitems.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

case class UvCount(windowEnd: Long, count: Long)

object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val url: URL = getClass.getResource("/UserBehavior.csv")
    println(url.getPath)
    val resultStream: DataStream[UvCount] = env.readTextFile(url.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(data => ("key", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())

     resultStream.print()

    env.execute("Bloom filter job")


  }

}

class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
}

class UvCountWithBloom extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{
  lazy val jedis = new Jedis("localhost",6379)
  lazy val bloom  = new Bloom( 1 << 29)
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {

    val storeKey: String = context.window.getEnd.toString
    var count:Long = 0L
    if(jedis.hget("count",storeKey) != null){
      count = jedis.hget("count",storeKey).toLong
    }
    val userId: String = elements.last._2.toString
    val offset: Long = bloom.hash(userId,61)
    val isExist: lang.Boolean = jedis.getbit(storeKey,offset)
    if(!isExist){
      jedis.setbit(storeKey,offset,true)
      jedis.hset("count",storeKey,(count +1 ).toString)
      out.collect(UvCount(storeKey.toLong,count + 1 ))
    }else{
      out.collect(UvCount(storeKey.toLong,count))
    }

  }
}


class Bloom(size:Long) extends Serializable {
  private val cap = size
  def hash(value:String,seed:Int):Long = {
    var result = 0
    for(i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }
    (cap - 1) & result

  }

}