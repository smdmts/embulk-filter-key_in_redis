package org.embulk.filter.key_in_redis.redis

import org.slf4j.Logger
import redis._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent._
import scala.util._
import scala.collection.mutable.ListBuffer
import better.files._
import org.velvia.MsgPackUtils._
import org.velvia.MsgPack

class Redis(setKey: String,
            host: String,
            port: Int,
            replicaHosts: Map[String, Int],
            db: Option[Int],
            loadOnMemory: Boolean,
            localCachePath: Option[String])(implicit logger: Logger) {
  implicit val actorSystem = akka.actor.ActorSystem(
    "redis-client",
    classLoader = Some(this.getClass.getClassLoader))

  lazy val cache: Set[String] = if (loadOnMemory) {
    logger.info(s"Using cache.")
    if (localCachePath.isDefined) {
      logger.info(s"Using local file cache.")
      val tmpFile = file"${localCachePath.get}"
      if (tmpFile.exists) {
        logger.info(
          s"Load local cache file start. ${tmpFile.path.toAbsolutePath.toString}")
        val record = unpackSeq(tmpFile.byteArray).map(_.toString).toSet
        logger.info(s"Load local cache file finished.")
        record
      } else {
        val record = loadAll()
        logger.info(
          s"Writing local cache file start. ${tmpFile.path.toAbsolutePath.toString}")
        tmpFile.touch()
        tmpFile.writeByteArray(MsgPack.pack(record))
        logger.info(s"Writing local cache file finished.")
        record.toSet
      }
    } else {
      loadAll().toSet
    }
  } else Set.empty

  val redisServers: Seq[RedisClient] = {
    val primary = RedisClient(host, port, db = db)
    val replica = replicaHosts.map {
      case (host: String, port: Int) =>
        RedisClient(host, port, db = db)
    }
    Seq(primary) ++ replica.toSeq
  }

  def redis: RedisClient = Random.shuffle(redisServers).head

  private def loadAll(): Seq[String] = {
    logger.info(s"Loading start.")
    import scala.concurrent.ExecutionContext.Implicits.global
    import ToFutureExtensionOps._
    val buffer = new ListBuffer[String]
    @tailrec
    def _scan(cursor: Int): Unit = {
      val task = redis.sscan[String](setKey, cursor, Option(500)).toTask
      val result = task.unsafePerformSync
      buffer.append(result.data: _*)
      if (result.index != 0) {
        _scan(result.index)
      }
    }
    _scan(0)
    logger.info(s"Loading finished. ${buffer.size}")
    buffer
  }

  def ping(): String = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val s: Future[String] = redis.ping()
    s.onComplete {
      case Success(result) => result
      case Failure(t) =>
        actorSystem.shutdown()
        throw t
    }
    Await.result(s, 10.minute)
  }

  def exists(values: Seq[String]): Map[String, Boolean] = {
    if (loadOnMemory) {
      values.map { v =>
        v -> cache.contains(v)
      }.toMap
    } else {
      import scala.concurrent.ExecutionContext.Implicits.global
      import ToFutureExtensionOps._
      val input = values.zipWithIndex.map(_.swap).toMap
      val transaction = redis.transaction()
      val f = values.map { v =>
        transaction.sismember(setKey, v)
      }
      transaction.exec()
      val results = Future
        .sequence(f)
        .toTask
        .unsafePerformSync
        .zipWithIndex
        .map(_.swap)
        .toMap
      results.map {
        case (index, result) =>
          input(index) -> result
      }
    }
  }

  def close(): Unit = {
    redis.stop()
    // wait for stopping.
    Thread.sleep(1000)
    actorSystem.shutdown()
  }

}
