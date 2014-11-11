local actor = require 'luact.actor'
local clock = require 'luact.clock'
local exception = require 'pulpo.exception'
local _M = {}

local supervisor_index = {}
local supervisor_mt = { __index = supervisor_index }
_M.opts = {
	maxt = 5.0, maxr = 5, -- torelate 5 failure in 5.0 seconds
	count = 1,
	always = false, 
}

-- hook system event
function supervisor_index:__actor_event__(act, event, ...)
	-- print('sv event == ', act, event, ...)
	if event == actor.EVENT_LINK_DEAD then
		local id, reason = select(1, ...)
		if reason or self.opts.always then
			act:unlink(id)
			self:restart_child(id)
			return true -- handled. default behavior will skip
		end
	end
end
local function err_handler(e)
	return exception.new('actor_runtime_error', e)
end
function supervisor_index:restart_child(died_actor_id)
	if not self.restart then
		self.first_restart = clock.get()
		self.restart = 1
	else
		self.restart = self.restart + 1
		local now = clock.get()
		if now - self.first_restart < self.opts.maxt then
			if self.restart >= self.opts.maxr then
				for idx,id in ipairs(self.restarting) do
					actor._set_restart_result(id, false) -- indicate restart failure
				end
				actor.destroy(actor.of(self))
				return
			else
				self.first_restart = now
				self.restart = 1
			end
		end
	end
	table.insert(self.restarting, died_actor_id)
	local supervise_opts = { supervised = true, uuid = died_actor_id }
	local ok, r = xpcall(actor.new_link_with_opts, err_handler, actor.of(self), supervise_opts, self.ctor, unpack(self.args))
	if not ok then
		-- retry restart.
		actor.of(self):restart_child(died_actor_id, reason)
	else -- indicate restart success
		actor._set_restart_result(died_actor_id, true)
		for idx,id in ipairs(self.restarting) do
			if id:equals(died_actor_id) then
				table.remove(self.restarting, idx)
				break
			end
		end
	end
end
local supervise_opts = { supervised = true }
function supervisor_index:start_children()
	while #self.children < self.opts.count do
		local child = actor.new_link_with_opts(actor.of(self), supervise_opts, self.ctor, unpack(self.args))
		table.insert(self.children, child)
	end
	return #self.children == 1 and self.children[1] or self.children
end

local function supervisor(ctor, opts, ...)
	local sv = setmetatable({
		ctor = ctor, args = {...}, 
		children = {}, restarting = {}, 
		opts = opts and setmetatable(opts, _M.opts) or _M.opts,
	}, supervisor_mt)
	return sv
end

-- module function
--[[
	opts 
		maxt,maxr : restart frequency check. if child actor restarts *maxr* times in *maxt* seconds, 
					supervisor dies
		count : how many child will be created (and supervised)
--]]
function _M.new(ctor, opts, ...)
	local sva = actor.new(supervisor, ctor, opts, ...)
	return sva:start_children(), sva
end

return setmetatable(_M, {
	__call = function (t, ctor, ...)
		return _M.new(ctor, nil, ...)
	end,
})
