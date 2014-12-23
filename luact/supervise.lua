local actor = require 'luact.actor'
local clock = require 'luact.clock'
local uuid = require 'luact.uuid'
local exception = require 'pulpo.exception'
local _M = {}

local supervisor_index = {}
local supervisor_mt = { __index = supervisor_index }
_M.opts = {
	maxt = 5.0, maxr = 5, -- torelate 5 failure in 5.0 seconds
	count = 1,
	always = false, 
}
local opts_mt = { __index = _M.opts }

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
function supervisor_index:add_restarting_id(add)
	for idx,id in ipairs(self.restarting) do
		if uuid.equals(id, add) then
			return
		end
	end
	table.insert(self.restarting, add)
end
function supervisor_index:remove_restarting_id(rem)
	for idx,id in ipairs(self.restarting) do
		if uuid.equals(id, rem) then
			table.remove(self.restarting, idx)
		end
	end
end
function supervisor_index:clear_uuids()
	for i=1,#self.restarting,1 do
		self.restarting[i] = nil
	end
	for i=1,#self.children,1 do
		local id = self.children[i]
		if _M.DEBUG then logger.warn('set restart failure:', id) end
		actor._set_restart_result(id, false) -- indicate restart failure
		self.children[i] = nil
	end		
end

local function err_handler(e)
	return exception.new('actor_error', e)
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
				if _M.DEBUG then logger.warn('restart_child fails:', self.restart, self.opts.maxr, #self.restarting) end
				self:clear_uuids()
				actor.destroy(actor.of(self))
				return
			end
		else
			self.first_restart = now
			self.restart = 1
		end
	end
	if _M.DEBUG then logger.warn('restart_child try:', self.restart, died_actor_id, #self.restarting) end
	self:add_restarting_id(died_actor_id)
	local supervise_opts = { supervised = true, uuid = died_actor_id }
	local ok, r = xpcall(actor.new_link_with_opts, err_handler, actor.of(self), supervise_opts, self.ctor, unpack(self.args))
	if _M.DEBUG then logger.warn('restart_child result:', ok, r, self, #self.restarting) end
	if not ok then
		-- retry restart.
		actor.of(self):notify_restart_child(died_actor_id, reason)
	else -- indicate restart success
		actor._set_restart_result(died_actor_id, true)
		self:remove_restarting_id(died_actor_id)
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
		opts = opts and setmetatable(opts, opts_mt) or _M.opts,
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
	__call = function (t, ctor, opts, ...)
		return _M.new(ctor, opts, ...)
	end,
})
