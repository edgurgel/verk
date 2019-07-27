local job = ARGV[1]

if job then
  -- The job contents are already available, find the destination queue name.
  local queue = cjson.decode(job)["queue"]
  if queue then
    local queue_key = string.format('verk:queue:%s', queue)

    redis.call("XADD", queue_key, "*", "job", job)
    redis.call("ZREM", KEYS[1], job)
  end
end

return job
