local counter = redis.call('INCRBY', KEYS[1], ARGV[1]);
local prevCounter = redis.call('GET', KEYS[2]);

prevCounter = (prevCounter ~= nil and tonumber(prevCounter) or 0) * tonumber(ARGV[3])

if prevCounter + counter > tonumber(ARGV[2])
then
  return tostring(redis.call('INCRBY', KEYS[1], -ARGV[1]) + prevCounter);
else
  return tostring(counter - tonumber(ARGV[1]));
end
