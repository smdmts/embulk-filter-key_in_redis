package org.embulk.filter.key_in_redis.actor

import akka.actor.{ActorRef, ActorSystem, Props}

object Actors {
  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem(
    "redis-register",
    classLoader = Some(this.getClass.getClassLoader))
  val register: ActorRef =
    actorSystem.actorOf(Props(classOf[Register]), "filtering_actor")

}
