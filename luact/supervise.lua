local luact = require 'luact'
local actor = require 'luact.actor'
local _M = {}

local supervisor_index = {}
local supervisor_mt = {__index = supervisor_index }
local supervisor_opts_mt = {
	maxt = 5.0, maxr = 5,
	distribute = false,
}
function supervisor_index:sys_event(event, ...)
	if event == 'destroy' then
		self:restart_child()
		return
	end
	actor.default_sys_event(self, event, ...)
end
function supervisor_index:restart_child()
	if not self.restart then
		self.first_restart = pulpo.util.clock()
		self.restart = 1
	else
		self.restart = self.restart + 1
		if self.maxt and pulpo.util.clock() - self.first_restart < self.maxt then
			if self.restart >= self.maxr then
				actor.destroy(self.__actor)
				return
			else
				self.first_restart = pulpo.util.clock()
				self.restart = 1
			end
		end
	end
	self:start_child()
end
function supervisor_index.start_child()
	local factory = self.opts.distribute and luact or actor
	self.child = factory.new_link(self.__actor, self.ctor, unpack(self.args))
end

local function supervisor(ctor, opts, ...)
	return setmetatable({
		ctor = ctor, args = {...}, 
		opts = opts and setmetatable(opts, supervisor_opts_mt) or supervisor_opts_mt,
	}, supervisor_mt)
end

-- module function
--[[
	opts 
		maxt,maxr : restart frequency check. if child actor restarts *maxr* times in *maxt* seconds, 
					supervisor dies
		distribute : if its true, luact tries to create actor in another thread or node
]]
function _M.start(ctor, opts, ...)
	local factory = opts.distribute and luact or actor
	local sv = actor.new(supervisor, ctor, opts, ...)
	sv:start_child()
	return sv
end

return setmetatable(_M, {
	__call = function (ctor, ...)
		return _M.start(ctor, nil, ...)
	end,
})
