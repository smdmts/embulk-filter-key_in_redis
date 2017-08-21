package org.embulk.filter.key_in_redis.redis

import org.slf4j.Logger

import scala.collection.mutable

case class Cache(loadFromStorage: () => mutable.Set[String])(implicit logger: Logger) {
  private val cache = loadFromStorage()
  def contains(value: String): Boolean = cache.contains(value)
}
