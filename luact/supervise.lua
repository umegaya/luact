local actor = require 'luact.actor'
local clock = require 'luact.clock'
local _M = {}

local supervisor_index = {}
local supervisor_mt = {__index = supervisor_index }
_M.opts = {
	maxt = 5.0, maxr = 5, -- torelate 5 failure in 5.0 seconds
	count = 1,
	distribute = false,
}
-- hook system event
function supervisor_index:__sys_event__(event, ...)
	if event == actor.sys_event.LINK_DEAD then
		self:restart_child(...)
		return true -- handled. default behavior will skip
	end
end
function supervisor_index:restart_child(died_actor_id, reason)
	if not self.restart then
		self.first_restart = clock.get()
		self.restart = 1
	else
		self.restart = self.restart + 1
		local now = clock.get()
		if now - self.first_restart < self.opts.maxt then
			if self.restart >= self.opts.maxr then
				actor.destroy(actor.id_of(self))
				return
			else
				self.first_restart = now
				self.restart = 1
			end
		end
	end
	-- TODO : if error caused, this supervisor died. preparing supervisor of supervisors?
	actor.new_link_with_id(actor.id_of(self), died_actor_id, self.ctor, unpack(self.args))
end
function supervisor_index:start_children()
	while #self.children < self.opts.count do
		local child = actor.new_link(actor.id_of(self), self.ctor, unpack(self.args))
		table.insert(self.children, child)
	end
end

local function supervisor(ctor, opts, ...)
	return setmetatable({
		ctor = ctor, args = {...}, 
		opts = opts and setmetatable(opts, _M.opts) or _M.opts,
	}, supervisor_mt)
end

-- module function
--[[
	opts 
		maxt,maxr : restart frequency check. if child actor restarts *maxr* times in *maxt* seconds, 
					supervisor dies
		count : how many child will be created (and supervised)
--]]
function _M.new(ctor, opts, ...)
	local sv = actor.new(supervisor, ctor, opts, ...)
	sv:start_children()
	return sv
end

return setmetatable(_M, {
	__call = function (ctor, ...)
		return _M.new(ctor, nil, ...)
	end,
})
