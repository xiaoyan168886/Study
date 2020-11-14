package flink.analysis.check


import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

case class OrderEvent(orderId: Long, eventType: String, eventTime:Long)
case class OrderResult(orderId: Long, eventType: String)

object OrderTimeOut {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val url = getClass().getResource("/OrderLog.csv")
    val orderStream: DataStream[OrderEvent] = env.readTextFile(url.getPath)
      .map(order => {
        val orderArray: Array[String] = order.split(",")
        OrderEvent(orderArray(0).trim.toLong, orderArray(1).trim, orderArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)

    val orderPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    val orderTimeOutTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeOut")

    val orderPatternStream: PatternStream[OrderEvent] = CEP.pattern(orderStream.keyBy(_.orderId), orderPattern)

    val resultStream: DataStream[OrderResult] = orderPatternStream.select(orderTimeOutTag) {
      (pattern: collection.Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val createOrder: Option[Iterable[OrderEvent]] = pattern.get("begin")
        OrderResult(createOrder.get.iterator.next().orderId, "order timeout")
      }
    } {
      pattern: collection.Map[String, Iterable[OrderEvent]] => {
        val payOrder: Option[Iterable[OrderEvent]] = pattern.get("follow")
        OrderResult(payOrder.get.iterator.next().orderId, "order success")
      }
    }
    val timeOutResult: DataStream[OrderResult] = resultStream.getSideOutput(orderTimeOutTag)
    resultStream.print("success:")
    timeOutResult.print("timeout:")
    env.execute("cep job")
  }
}