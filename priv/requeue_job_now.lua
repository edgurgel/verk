local job = ARGV[1]

if job then
  -- The job contents are already available, find the destination queue name.
  local queue = cjson.decode(job)["queue"]
  if queue then
    local queue_key = string.format('queue:%s', queue)

    -- Don't requeue the job unless it has been added to the destination queue.
    local added = redis.call("LPUSH", queue_key, job)
    if added ~= 0 then
      redis.call("ZREM", KEYS[1], job)
    else
      -- Signifies that the job could not be added to the destination
      -- queue and no work was done.
      return nil
    end
  end
end

return job
