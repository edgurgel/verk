local job = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "limit", 0, 1)[1]

if job then
  local decoded = cjson.decode(job)
  local queue = decoded["queue"]
  if queue then
    local queue_key = string.format('queue:%s', queue)
    local enqueued_at = decoded["enqueued_at"]
    if not enqueued_at then
      decoded["enqueued_at"] = ARGV[1]
    end
    local reencoded = cjson.encode(decoded)
    redis.call("LPUSH", queue_key, reencoded)
    redis.call("ZREM", KEYS[1], job)
    job = reencoded
  end
end

return job

