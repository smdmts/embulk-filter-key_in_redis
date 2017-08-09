package org.embulk.filter.key_in_redis

import org.embulk.config.{ConfigSource, TaskSource}
import org.embulk.filter.key_in_redis.redis.Redis
import org.embulk.spi
import org.embulk.spi._

class KeyInRedisFilterPlugin extends FilterPlugin {

  override def transaction(config: ConfigSource,
                           inputSchema: Schema,
                           control: FilterPlugin.Control): Unit = {
    val task = config.loadConfig(classOf[PluginTask])
    KeyInRedisFilterPlugin.createRedisInstance(task)
    KeyInRedisFilterPlugin.redis.foreach(_.ping())
    control.run(task.dump(), inputSchema)
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
        KeyInRedisFilterPlugin.createRedisInstance(task)
    }
    PageOutput(task, outputSchema, output)
  }
}

object KeyInRedisFilterPlugin {
  var redis: Option[Redis] = None
  def createRedisInstance(task: PluginTask): Unit = {
    KeyInRedisFilterPlugin.redis = Some(
      Redis(task.getRedisSetKey, task.getHost, task.getPort, {
        if (task.getDb.isPresent) Some(task.getDb.get())
        else None
      }))
  }
}
