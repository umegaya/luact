local luact = require 'luact.init'
local actor = require 'luact.actor'
local tentacle = require 'pulpo.tentacle'
local mod = require 'luact.module'
local clock = require 'luact.clock'
local serde = require 'luact.serde'
local json = serde[serde.kind.json]
local util = require 'pulpo.util'
local aws_util = require 'lua-aws.util'
local process = luact.process
local _M = (require 'pulpo.package').module('luact.defer.deploy_c')

local actor_dependency = {}
local deploy_queue = {}
local default = {
	update_per_resume = 100
}

-- deploy methods
local github_methods = {
	buf = ffi.new('char[256]'),	
}

function github_methods:filter(verb, headers, body)
	if headers["X-GitHub-Event"] ~= "push" then
		logger.info('web hook ignored by not from github')
		return false
	end
	local secret = self.opts.secret
	if secret then
		local sig = headers["X-Hub-Signature"]
		if not sig then
			logger.info('web hook ignored by not specifiying X-Hub-Signature')
			return false
		end
		local compute_sig = aws_util.hmac(secret, body, 'hex')
		if sig ~= compute_sig then
			logger.info('web hook ignored by signature not matched', sig, compute_sig)
			return false
		end
	else
		return false
	end
	body = json:unpack_from_string(body)
	if not body.repository.full_name:match(self.opts.repository) then
		return false
	end
	for i=1,#self.opts.branches do
		local b = self.opts.branches[i]
		if body.ref:match(b) then
			return body
		end
	end
	return false
end
function github_methods:change_files()
	-- compute git diff iterate through submodules
	return mod.diff_recursive(self.last_commit, 'HEAD', self.opts.diff_base)
end
function github_methods:pull(last_commit)
	if last_commit then
		self.last_commit = last_commit
	else
		local ec, output = process.execute(('cd %s && git log -1 --pretty="%%H"'):format(self.opts.diff_base))
		if ec ~= 0 then
			logger.warn('get current last commit fails', ec)
			return
		end
		self.last_commit = output:sub(1, -2)
	end
	local p = process.open(('cd %s && git pull'):format(self.opts.diff_base))
	while true do
		local ok, r = p:read(self.buf, 256)
		if not ok then
			assert(r == 0, "fail to git pull:"..tostring(r))
			break
		else
			logger.debug('output:', ffi.string(self.buf, ok))
		end
	end
end

local gitlab_methods = util.copy_table(github_methods)
function gitlab_methods:filter(verb, headers, body)
	-- alas, from gitlab payload, cannot get anything to verify sender :<
	return true
end

local deploy_methods = {
	["gitlab.com"] = gitlab_methods,
	["github.com"] = github_methods,
}

-- deploy src
local src_mt = {}
src_mt.__index = src_mt
function src_mt:run_update(opts)
::AGAIN::
	logger.info('start update task')
	self:pull(opts.last_commit)
	mod.invalidate_submodule_cache()
	local list = mod.compute_change_set(self:change_files())
	logger.info(#list, 'file(s) changed')
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
	logger.info('no more update task in queue. collect unused module table')
	mod.gc()
	self.thread = nil
	logger.info('update done')
end
function src_mt:update(opts)
	if not self.thread then
		self.thread = tentacle(self.run_update, self, opts)
	else
		table.insert(deploy_queue, opts)
	end
end


-- module functions
function _M.hook_commit(verb, header, body, opts)
	if _M.deploy_method:filter(verb, header, body) then
		local tmp = util.copy_table(default)
		opts = opts and util.merge_table(tmp, opts) or tmp
		_M.deploy_method:update(opts)
		return true
	else
		logger.info('commit payload received but ignored')
		return false
	end
end

function _M.actor_factory()
	return {
		deploy = _M.hook_commit,
	}
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