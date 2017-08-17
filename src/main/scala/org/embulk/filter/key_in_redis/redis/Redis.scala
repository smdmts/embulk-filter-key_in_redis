package org.embulk.filter.key_in_redis.redis

import redis.RedisClient

import scala.concurrent.duration._
import scala.concurrent._
import scala.util._

case class Redis(setKey: String, host: String, port: Int, db: Option[Int]) {
  implicit val actorSystem = akka.actor.ActorSystem(
    "redis-client",
    classLoader = Some(this.getClass.getClassLoader))
  val redis = RedisClient(host, port, db = db)

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
    import scala.concurrent.ExecutionContext.Implicits.global
    val input = values.zipWithIndex.map(_.swap).toMap
    val transaction = redis.transaction()
    val f = values.map { v =>
      transaction.sismember(setKey, v)
    }
    transaction.exec()
    val results = Await
      .result(Future.sequence(f), 10.minutes)
      .zipWithIndex
      .map(_.swap)
      .toMap
    results.map {
      case (index, result) =>
        input(index) -> result
    }
  }

  def close(): Unit = {
    redis.stop()
    // wait for stopping.
    Thread.sleep(1000)
    actorSystem.shutdown()
  }

}
