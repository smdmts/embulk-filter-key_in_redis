Embulk::JavaPlugin.register_filter(
  "key_in_redis", "org.embulk.filter.key_in_redis.KeyInRedisFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
