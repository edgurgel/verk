local job = ARGV[1]

if job then
  -- The job contents are already available, find the destination queue name.
  local queue = cjson.decode(job)["queue"]
  if queue then
    local queue_key = string.format('queue:%s', queue)

    -- Don't requeue the job unless its removal from current sorted set
    -- can be confirmed.
    local removed = redis.call("ZREM", KEYS[1], job)
    if removed == 1 then
      redis.call("LPUSH", queue_key, job)
    else
      -- Signifies that the job was not present in the initial queue
      -- and so no work was done.
      return nil
    end
  end
end

return job
