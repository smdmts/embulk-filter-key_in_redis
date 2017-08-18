package org.embulk.filter.key_in_redis

import org.embulk.config.{ConfigSource, TaskSource}
import org.embulk.filter.key_in_redis.redis.Redis
import org.embulk.spi
import org.embulk.spi._
import org.slf4j.Logger

class KeyInRedisFilterPlugin extends FilterPlugin {

  override def transaction(config: ConfigSource,
                           inputSchema: Schema,
                           control: FilterPlugin.Control): Unit = {
    val task = config.loadConfig(classOf[PluginTask])
    val taskSource = task.dump()
    KeyInRedisFilterPlugin.createRedisInstance(task)
    KeyInRedisFilterPlugin.redis.foreach(_.ping())
    if (task.getLoadOnMemory) {
      val cache =
        KeyInRedisFilterPlugin.redis.map(_.cache).getOrElse(Set.empty)
      taskSource.set(KeyInRedisFilterPlugin.cacheName, cache)
    }
    control.run(taskSource, inputSchema)
    KeyInRedisFilterPlugin.redis.foreach(_.close())
  }

  override def open(taskSource: TaskSource,
                    inputSchema: Schema,
                    outputSchema: Schema,
                    output: spi.PageOutput): PageOutput = {
    val task = taskSource.loadTask(classOf[PluginTask])
    KeyInRedisFilterPlugin.redis match {
      case Some(_) => // nothing to do
      case None => // for map reduce executor.
        val record = if (task.getLoadOnMemory) {
          taskSource.get(classOf[Set[String]],
                         KeyInRedisFilterPlugin.cacheName)
        } else Set.empty[String]
        KeyInRedisFilterPlugin.createRedisInstance(task, record)
    }
    PageOutput(task, outputSchema, output)
  }
}

object KeyInRedisFilterPlugin {
  lazy val cacheName = s"${this.getClass.getCanonicalName}-cache"
  implicit val logger: Logger = Exec.getLogger(classOf[KeyInRedisFilterPlugin])
  var redis: Option[Redis] = None
  def createRedisInstance(task: PluginTask,
                          cache: Set[String] = Set.empty): Unit = {
    KeyInRedisFilterPlugin.redis = Some(
      new Redis(task.getRedisSetKey, task.getHost, task.getPort, {
        if (task.getDb.isPresent) Some(task.getDb.get())
        else None
      }, task.getLoadOnMemory))
  }
}
