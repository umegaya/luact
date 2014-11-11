local actor = require 'luact.actor'
local mt = {
	__index = {
		ping = function (t, target, count)
			if count <= 0 then
				t.finished = true
			else
				target:notify_ping(actor.of(t), count - 1)
			end
		end,
		is_finished = function (t)
			return t.finished
		end,
	}
}
return setmetatable({finished = false}, mt)
