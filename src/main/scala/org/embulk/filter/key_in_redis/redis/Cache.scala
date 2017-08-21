package org.embulk.filter.key_in_redis.redis

import better.files._
import org.slf4j.Logger

import scala.collection.mutable

case class Cache(localCachePath: Option[String],
                 loading: () => mutable.Set[String])(implicit logger: Logger) {

  private var cache = mutable.Set[String]()
  private val fileOpt: Option[File] = localCachePath.map(v => file"$v")

  def cacheFileExists(): Boolean = fileOpt.exists(_.exists)
  def contains(value: String): Boolean = cache.contains(value)

  def get(): mutable.Set[String] = {
    if (cache.nonEmpty) {
      cache
    } else {
      if (cacheFileExists()) {
        cache = loading()
        cache
      } else {
        cache = loading()
        writeLocalCache()
        cache
      }
    }
  }

  private def writeLocalCache(): Unit = {
    logger.info(s"Writing local cache start.")
    var list = List.empty[String]
    fileOpt.foreach { file =>
      cache.foreach { v =>
        list = v :: list
        if (list.size == 10000) {
          file.appendLines(list: _*)
        }
      }
      if (list.nonEmpty) {
        file.appendLines(list: _*)
      }
    }
    logger.info(s"Writing local cache finished.")
  }

}
