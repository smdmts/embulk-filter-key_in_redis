package org.embulk.filter.key_in_redis.actor

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import org.embulk.filter.key_in_redis.KeyInRedisFilterPlugin
import org.embulk.filter.key_in_redis.redis.Redis
import org.embulk.filter.key_in_redis.row.Row
import org.embulk.spi.PageBuilder

import scala.concurrent._
import scala.concurrent.duration.Duration

class Register extends Actor {

  implicit val ec: ExecutionContextExecutor = context.system.dispatcher
  var rowList: List[Row] = List.empty[Row]
  lazy val redis: Redis =
    KeyInRedisFilterPlugin.redis.getOrElse(sys.error("redis is undefined."))

  override def receive: PartialFunction[Any, Unit] = {
    case row: Row =>
      rowList = row :: rowList
      if (rowList.size == 500) {
        addRecords(rowList)
      }
    case Counter(pb) =>
      val (owned, _) =
        rowList.partition(_.pageBuilder == pb)
      sender() ! owned.size
    case Add(row) =>
      row.addRecord()
      rowList = rowList.filter(_ == row)
    case Ignore(row) =>
      rowList = rowList.filter(_ == row)
  }

  private def addRecords(rows: List[Row]) = {
    val result = redis.exists(rows.map(_.matchKey))
    rows.foreach { row =>
      val f = result(row.matchKey)
      f.foreach { result =>
        if (!result) {
          implicit val timeout = Timeout(24, TimeUnit.HOURS)
          self ! Add(row)
        } else {
          self ! Ignore(row)
        }
      }
    }
  }
}

case class Add(row: Row)
case class Ignore(row: Row)
case class Counter(pageBuilder: PageBuilder)