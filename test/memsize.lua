local thread = require 'luact.thread'
thread.init()
if true then
local t = thread.create(function ()
 local thread = require 'luact.thread'
 while true do
  thread.sleep(1.0)
  print('ok')
 end
 return nil
end)
end

thread.sleep(5.0)
