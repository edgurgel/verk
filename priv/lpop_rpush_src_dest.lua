local batch_size = ARGV[1]
local source = KEYS[1]
local destination = KEYS[2]
local job_count = redis.call('LLEN', source)
local limit = job_count

if job_count > 0 then
  if job_count > 1000 then
    local limit = batch_size
  end

  for i=1, batch_size do
    redis.call("RPOPLPUSH", source, destination)
  end

  return limit
else
  return nil
end
