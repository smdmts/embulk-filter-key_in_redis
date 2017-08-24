package org.embulk.filter.key_in_redis.redis

import org.slf4j.Logger
import redis._
import org.embulk.filter.key_in_redis.actor.Actors._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent._
import scala.util._
import scala.collection.mutable

class Redis(setKey: String,
            host: String,
            port: Int,
            replicaHosts: Map[String, Int],
            db: Option[Int],
            loadOnMemory: Boolean)(implicit logger: Logger) {

  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher
  lazy val cacheInstance: Option[Cache] = if (loadOnMemory) {
    Some(Cache(() => loadAll()))
  } else None

  val redisServers: Seq[RedisClient] = {
    val primary = RedisClient(host, port, db = db)
    val replica = replicaHosts.map {
      case (host: String, port: Int) =>
        RedisClient(host, port, db = db)
    }
    Seq(primary) ++ replica.toSeq
  }

  def redis: RedisClient = Random.shuffle(redisServers).head

  def loadAll(): mutable.Set[String] = {
    logger.info(s"Loading from Redis start.")
    import org.embulk.filter.key_in_redis.ToFutureExtensionOps._
    val buffer = mutable.Set.empty[String]
    @tailrec
    def _scan(cursor: Int): Unit = {
      val task = redis.sscan[String](setKey, cursor, Option(500)).toTask
      val result = task.unsafePerformSync
      result.data.foreach { v =>
        buffer.add(v)
      }
      if (result.index != 0) {
        _scan(result.index)
      }
    }
    _scan(0)
    logger.info(s"Loading from Redis finished. record size is ${buffer.size}")
    buffer
  }

  def ping(): String = {
    val s: Future[String] = redis.ping()
    s.onComplete {
      case Success(result) =>
        result
      case Failure(t) =>
        actorSystem.shutdown()
        throw t
    }
    Await.result(s, 10.minute)
  }

  def keyExists(): Unit = {
    val s: Future[Boolean] = redis.exists(setKey)
    s.onComplete {
      case Success(_) =>
      case Failure(t) =>
        actorSystem.shutdown()
        throw t
    }
    val result = Await.result(s, 10.minute)
    if (!result) {
      actorSystem.shutdown()
      throw sys.error(s"key not found in redis. $setKey")
    }
  }

  def exists(values: Seq[String]): Map[String, Future[Boolean]] =
    cacheInstance match {
      case Some(cached) =>
        values.map { v =>
          v -> Future.successful(cached.contains(v))
        }.toMap
      case None =>
        val transaction = redis.transaction()
        val f = values.map { v =>
          v -> transaction.sismember(setKey, v)
        }.toMap
        transaction.exec()
        f
    }

  def close(): Unit = {
    redis.stop()
    // wait for stopping.
    Thread.sleep(1000)
    actorSystem.shutdown()
  }

}
