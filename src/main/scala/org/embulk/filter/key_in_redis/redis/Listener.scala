package org.embulk.filter.key_in_redis.redis

import akka.actor.{Actor, ActorSystem}
import redis.RedisClient

import scala.collection.mutable
import scala.concurrent.Future

case class Listener(setKey:String, detector: () => RedisClient) extends Actor {
  implicit val actorSystem: ActorSystem = context.system
  val buffer = mutable.Set.empty[Records]
  val results = mutable.Map.empty[String, Future[Boolean]]
  var recordCounter = 0

  override def receive = {
    case record:Records =>
      buffer.add(record)
      recordCounter += record.values.size
      if (recordCounter > 10000) {
        val transaction = detector().transaction()
        buffer.flatMap(_.values).map { v =>
          val result = transaction.sismember(setKey, v)
          results.put(v, result)
        }
        transaction.exec()
      }
  }

}

case class Records(values: Seq[String])
