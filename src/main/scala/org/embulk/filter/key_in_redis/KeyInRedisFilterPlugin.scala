package org.embulk.filter.key_in_redis

import org.embulk.config.{ConfigSource, TaskSource}
import org.embulk.spi.{FilterPlugin, PageOutput, Schema}

class KeyInRedisFilterPlugin extends FilterPlugin {
  override def open(taskSource: TaskSource,
                    inputSchema: Schema,
                    outputSchema: Schema,
                    output: PageOutput): PageOutput = ???

  override def transaction(config: ConfigSource,
                           inputSchema: Schema,
                           control: FilterPlugin.Control): Unit = ???
}
