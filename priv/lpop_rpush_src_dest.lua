local size = tonumber(ARGV[1])
local jobs = redis.call('LRANGE', KEYS[1], 0, size-1)
local len_before = redis.call('LLEN', KEYS[1])

if table.getn(jobs) > 0 then
  local queue_key = KEYS[2]
  redis.call('RPUSH', queue_key, unpack(jobs))

  redis.call('LTRIM', KEYS[1], size, -1)
end
local len_after = redis.call('LLEN', KEYS[1])
return { len_after, len_before - len_after }
