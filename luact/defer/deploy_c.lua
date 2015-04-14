local actor = require 'luact.actor'
local tentacle = require 'pulpo.tentacle'
local mod = require 'luact.module'
local clock = require 'luact.clock'
local util = require 'pulpo.util'
local _M = (require 'pulpo.package').module('luact.defer.deploy_c')

local actor_dependency = {}
local deploy_queue = {}
local default = {
	update_per_resume = 100
}

-- deploy methods
local deploy_methods = {
	["github.com"] = {
		buf = ffi.new('char[256]'),
		filter = function (self, payload)
			if payload.header["X-GitHub-Event"] ~= "push" then
				return false
			end
			if not payload.body.repository.full_name:match(self.opts.repository) then
				return false
			end
			for i=1,#self.opts.branches do
				local b = self.opts.branches[i]
				if payload.body.ref:match(b) then
					return true
				end
			end
			return false
		end,
		change_files = function (self, last_commit)
			-- compute git diff iterate through submodules
			return mod.diff_recursive(last_commit, 'HEAD', self.opts.diff_base)
		end,
		pull = function (self)
			local p = process.open(('cd %s && git pull'):format(self.opts.diff_base))
			while true do
				local ok, r = p:read(self.buf, 256)
				if not ok then
					assert(r ~= 0, "fail to git pull:"..tostring(r))
					break
				else
					print('output:', ffi.string(self.buf, ok))
				end
			end
		end,
	}
}

-- deploy src
local src_mt = {}
src_mt.__index = src_mt
function src_mt:run_update(opts)
::AGAIN::
	local last_commit = opts.last_commit or 
		process.execute(('cd %s && git log -1 --pretty="%H"'):format(self.opts.diff_base))
	self:pull()
	mod.invalidate_submodule_cache()
	local list = mod.compute_change_set(self:change_files(last_commit))
	local count = 0
	for f,_ in pairs(list) do
		local d = actor_dependency[f]
		if d then
			for j=#d,1,-1 do
				actor.destroy(d[j], "update")
				table.remove(d)
				count = count + 1
				if count > opts.update_per_resume then
					clock.sleep(0.1)
					count = 0
				end
			end
		end
	end
	if #deploy_queue > 0 then
		opts = unpack(table.remove(deploy_queue, 1))
		for i=1,#deploy_queue do
			deploy_queue[i] = nil
		end
		goto AGAIN
	end
	mod.gc()
end
function src_mt:update(opts)
	if not self.thread then
		self.thread = tentacle(self.run_update, self, opts)
		self.thread = nil
	else
		table.insert(deploy_queue, opts)
	end
end


-- module functions
function _M.hook_commit(payload, opts)
	if _M.deploy_method:filter(payload) then
		local tmp = util.copy_table(default)
		opts = util.merge_table(tmp, opts)
		_M.deploy_method:update(opts)
	else
		logger.info('commit payload received but ignores')
	end
end

function _M.set_actor_dependency(actor, src)
	if not actor_dependency[src] then
		actor_dependency[src] = {}
	end
	table.insert(actor_dependency[src], actor)
end

local list = {"pull", "apply", "filter"}
function _M.config_method(method, opts)
	local obj = deploy_methods[method]
	for i=1,#list do
		local name = list[i]
		if opts[name] then
			obj[name] = opts[name]
		end
	end
	obj.opts = opts
	_M.deploy_method = setmetatable(obj, src_mt)
end

return _M