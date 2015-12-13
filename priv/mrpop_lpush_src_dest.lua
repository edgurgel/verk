local jobs = {}
local n_jobs = tonumber(ARGV[1])
local n = 0

repeat
  local job = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
  if job then
    n = n + 1
    table.insert(jobs, job)
  else
    break
  end
until n == n_jobs

return jobs
