package org.embulk.filter.key_in_redis.actor

import akka.actor._
import org.embulk.filter.key_in_redis.KeyInRedisFilterPlugin
import org.embulk.filter.key_in_redis.redis.Redis
import org.embulk.filter.key_in_redis.row.Row
import org.embulk.spi.PageBuilder

import scala.collection.mutable
import scala.concurrent._

class Register extends Actor {

  implicit val ec: ExecutionContextExecutor = context.system.dispatcher
  var rowList: List[Row] = List.empty[Row]
  lazy val redis: Redis =
    KeyInRedisFilterPlugin.redis.getOrElse(sys.error("redis is undefined."))
  val counter: mutable.Map[PageBuilder, Int] = mutable.Map[PageBuilder, Int]()

  override def receive: PartialFunction[Any, Unit] = {
    case row: Row =>
      rowList = row :: rowList
      counter.put(row.pageBuilder, counter.getOrElse(row.pageBuilder, 0) + 1)
      if (rowList.size == 500) {
        addRecords(rowList)
        rowList = List.empty[Row]
      }
    case Counter(pb) =>
      sender() ! counter.getOrElse(pb, 0)
    case ForceWrite(pb) =>
      val (owned, other) =
        rowList.partition(_.pageBuilder == pb)
      addRecords(owned)
      rowList = other
    case Add(row) =>
      counter.put(row.pageBuilder, counter(row.pageBuilder) - 1)
      row.addRecord()
    case Ignore(row) =>
      counter.put(row.pageBuilder, counter(row.pageBuilder) - 1)
    case TotalCount =>
      sender() ! counter.foldLeft[Int](0) {
        case (total, (_, counter: Int)) =>
          total + counter
      }
  }

  private def addRecords(rows: List[Row]): Unit = {
    redis.exists(rows.map(_.matchKey)).foreach { resultMap =>
      rows.foreach { row =>
        val result = resultMap(row.matchKey)
        if (!result) {
          self ! Add(row)
        } else {
          self ! Ignore(row)
        }
      }
    }
  }
}

case object TotalCount
case class Add(row: Row)
case class Ignore(row: Row)
case class Counter(pageBuilder: PageBuilder)
case class ForceWrite(pageBuilder: PageBuilder)
