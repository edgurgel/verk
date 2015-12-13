local job = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "limit", 0, 1)[1]

if job then
  local queue = cjson.decode(job)["queue"]
  if queue then
    local queue_key = string.format('queue:%s', queue)
    redis.call("LPUSH", queue_key, job)
    redis.call("ZREM", KEYS[1], job)
  end
end

return job

