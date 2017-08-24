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
    case Finish(pb) =>
      val (finishRecords, _) =
        rowList.partition(_.pageBuilder == pb)
      Await.result(addRecords(finishRecords), Duration.Inf)
      pb.finish()
      sender() ! Applied()
    case Close(pb) =>
      val (finishRecords, _) =
        rowList.partition(_.pageBuilder == pb)
      Await.result(addRecords(finishRecords), Duration.Inf)
      pb.close()
      sender() ! Applied()
    case Write(row) =>
      row.addRecord()
      rowList = rowList.filter(_ == row)
  }

  private def addRecords(rows: List[Row]): Future[List[_]] = {
    val result = redis.exists(rows.map(_.matchKey))
    val writer = rows.map { row =>
      val resultFuture = result(row.matchKey)
      resultFuture.map { result =>
        if (!result) {
          implicit val timeout = Timeout(24, TimeUnit.HOURS)
          self ? Write(row)
        }
      }
    }
    Future.sequence(writer)
  }
}

case class Write(row: Row)
case class Finish(pageBuilder: PageBuilder)
case class Close(pageBuilder: PageBuilder)
case class Applied()
