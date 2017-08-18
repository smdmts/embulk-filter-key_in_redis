package org.embulk.filter.key_in_redis

import com.google.common.base.Optional
import org.embulk.config.{Config, ConfigDefault, Task}
import org.embulk.spi.time.TimestampFormatter

trait PluginTask extends Task with TimestampFormatter.Task {

  @Config("redis_set_key")
  def getRedisSetKey: String

  @Config("match_as_md5")
  @ConfigDefault("false")
  def getMatchAsMD5: Boolean

  @Config("load_on_memory")
  @ConfigDefault("false")
  def getLoadOnMemory: Boolean

  @Config("local_cache_path")
  @ConfigDefault("null")
  def getLocalCachePath: Optional[String]

  @Config("key_with_index")
  @ConfigDefault("{}")
  def getKeyWithIndex: java.util.Map[String, String]

  @Config("json_key_with_index")
  @ConfigDefault("{}")
  def getJsonKeyWithIndex: java.util.Map[String, String]

  @Config("appender")
  @ConfigDefault("\"-\"")
  def getAppender: String

  @Config("host")
  @ConfigDefault("\"127.0.0.1\"")
  def getHost: String

  @Config("port")
  @ConfigDefault("6379")
  def getPort: Int

  @Config("db")
  @ConfigDefault("null")
  def getDb: Optional[Int]

}
