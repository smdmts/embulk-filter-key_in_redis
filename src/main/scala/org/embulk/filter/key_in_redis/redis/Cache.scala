package org.embulk.filter.key_in_redis.redis

import java.io.{BufferedWriter, FileWriter, PrintWriter}

import better.files._
import org.slf4j.Logger

import scala.collection.mutable

case class Cache(
    localCachePath: Option[String],
    loadFromStorage: () => mutable.Set[String])(implicit logger: Logger) {

  private var cache = get()
  def fileOpt: Option[File] = localCachePath.map(v => file"$v")

  def cacheFileExists(): Boolean =
    if (fileOpt.nonEmpty) {
      fileOpt.get.exists
    } else false

  def contains(value: String): Boolean = cache.contains(value)

  def get(): mutable.Set[String] =
    if (cacheFileExists()) {
      logger.info(s"Local cache loading start.")
      val record = loadFromLocalCache()
      logger.info(
        s"Local cache loading finished. record size is ${record.size}. ")
      record
    } else {
      cache = loadFromStorage()
      writeLocalCache(cache)
      cache
    }

  def loadFromLocalCache(): mutable.Set[String] = {
    fileOpt.map { file =>
      val records = mutable.Set[String]()
      file.lines.foreach { line =>
        records.add(line)
      }
      records
    } getOrElse mutable.Set.empty[String]
  }

  private def writeLocalCache(record: mutable.Set[String]): Unit = {
    fileOpt.foreach { file =>
      logger.info(s"Writing local cache start.")
      val pw = new PrintWriter(new BufferedWriter(new FileWriter(file.toJava)))
      record.foreach { v =>
        pw.println(v)
      }
      pw.close()
      logger.info(
        s"Writing local cache finished. record size is ${record.size}")
    }
  }

}
