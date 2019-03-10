local group = ARGV[1]
local min_idle = ARGV[3]
local id = ARGV[2]
local stream = KEYS[1]

local result = redis.call('XCLAIM', stream, group, "reclaimer", min_idle, id)

-- non-empty table?
if next(result) then
  for k, v in pairs(result) do
    if v then
      redis.call('XADD', stream, '*', 'job', v[2][2])
      redis.call('XACK', stream, group, id)
      redis.call('XDEL', stream, id)
    end
  end
  return 1
else
  return 0
end
