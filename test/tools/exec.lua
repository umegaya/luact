local event = luact.event
local tentacle = luact.tentacle
local clock = luact.clock
local memory = luact.memory
local exception = luact.exception
local util = luact.util
local thread_id = luact.thread_id
local machine_id = luact.machine_id

assert(event and tentacle and clock and memory and thread_id and machine_id and exception and util)
clock.sleep(1)

if thread_id == 1 then
	luact.stop()
end
