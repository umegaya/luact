local actor = require 'luact.actor'
local _M = {}

local supervisor_index = {}
local supervisor_mt = {__index = supervisor_index }
_M.opts = {
	maxt = 5.0, maxr = 5, -- torelate 5 failure in 5.0 seconds
	count = 1,
	distribute = false,
}
function supervisor_index:sys_event(event, ...)
	if event == 'destroy' then
		self:restart_child(...)
		return
	end
	actor.default_sys_event(self, event, ...)
end
function supervisor_index:restart_child(died_actor, reason)
	if not self.restart then
		self.first_restart = pulpo.util.clock()
		self.restart = 1
	else
		self.restart = self.restart + 1
		if pulpo.util.clock() - self.first_restart < self.opts.maxt then
			if self.restart >= self.opts.maxr then
				actor.destroy(self.__actor)
				return
			else
				self.first_restart = pulpo.util.clock()
				self.restart = 1
			end
		end
	end
	-- TODO : if error caused, this supervisor died. preparing supervisor of supervisors?
	actor.new_link_with_id(self.__actor, died_actor, self.ctor, unpack(self.args))
end
function supervisor_index:start_children()
	while #self.children < self.opts.count do
		local child = actor.new_link(self.__actor, self.ctor, unpack(self.args))
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
