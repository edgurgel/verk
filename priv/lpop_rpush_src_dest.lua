local jobs = redis.call('LRANGE', KEYS[1], 0, -1)

if table.getn(jobs) > 0 then
  local queue_key = KEYS[2]
  redis.call("RPUSH", queue_key, unpack(jobs))

  redis.call('DEL', KEYS[1])
end

return table.getn(jobs)
