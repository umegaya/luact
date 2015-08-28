local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
local tentacle = require 'pulpo.tentacle'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'
local event = require 'pulpo.event'
local util = require 'pulpo.util'

local clock = require 'luact.clock'
local uuid = require 'luact.uuid'
local rio = require 'luact.util.rio'
local rpc = require 'luact.cluster.raft.rpc'
local actor_module = require 'luact.actor'

local _M = {}

ffi.cdef[[
typedef struct luact_raft_replicator {
	uint64_t next_idx, match_idx, added_idx;
	uint8_t failures, added, padd[2];
	double last_access, heartbeat_span_sec;
} luact_raft_replicator_t;
]]

local replicator_index = {}
local replicator_mt = {
	__index = replicator_index
}
local cache = {}
function replicator_index:init(state)
	self.next_idx = state:last_index() + 1
	self.match_idx = 0
	self.heartbeat_span_sec = state.opts.heartbeat_timeout_sec
end
function replicator_index:fin()
	self:stop_append()
	table.insert(cache, self)
end
function replicator_index:start_append(added_idx)
	self.added = 1
	self.added_idx = added_idx
end
function replicator_index:stop_append()
	self.added = 0
	self.added_idx = 0
end
function replicator_index:start(controller, actor, state, activate)
	self:init(state)
	if activate then self:start_append(state:last_applied_index()) end
	local hbev = tentacle(self.run_heartbeat, self, actor, state)
	local runev = tentacle(self.run, self, controller, actor, state, hbev)
	state:kick_replicator()
	return runev
end
local function err_handler(e)
	--[[if type(e) == 'table' then
		logger.report('raft', 'replicate', e)
	else
		logger.error('raft', 'replicate', tostring(e))
	end]]
	return e
end
function replicator_index:run_replication(t)
	t.running = true
	local ok, r = xpcall(self.replicate, err_handler, self, t.controller, t.actor, t.state)
	if not ok then
		logger.report('replicate error', t.actor, r)
	end
	t.rep_thread = nil -- this thread will be stop without canceling.
	if (not ok) and r:is('raft_not_found') then
		-- we can remove this replicator, because raft_not_found only returns when raft object will removed by destination node, 
		-- that means destination node process remove_replica_set log correctly.
		t.controller:stop_replicator(t.actor)
	end
	t.running = false
end
function replicator_index:run(controller, actor, state, hbev)
	event.select({
		self = self, 
		actor = actor, 
		controller = controller, 
		state = state, 
		hb_thread = hbev, 
		[state.ev_close] = function (t)
			t.self:fin()
			return true
		end,
		[hbev] = function (t)
			t.controller:stop_replicator(t.actor)
			return true
		end,
		[state.ev_log] = function (t, tp, ...)
			if tp == 'add' then
				if not t.running then
					-- logger.warn('run repl thread to', t.actor)
					t.rep_thread = tentacle(t.self.run_replication, t.self, t)
				end
			elseif tp == 'stop' then
				local obj = ({...})[2]
				if (not obj) or (obj == self) then
					local ok, r
					if t.rep_thread then
						logger.warn(state:gid_for_log(), 'stop repl thread to', t.actor, t, tostring(t.rep_thread[1]), coroutine.status(t.rep_thread[1]))
						ok, r = pcall(tentacle.cancel, t.rep_thread)
						if not ok then
							logger.error('cancel repl coroutine error:', r)
						end
					end
					if t.hb_thread then
						logger.warn(state:gid_for_log(), 'stop hb thread to', t.actor, t, tostring(t.hb_thread[1]), coroutine.status(t.hb_thread[1]))
						ok, r = pcall(tentacle.cancel, t.hb_thread)
						if not ok then
							logger.error('cancel hb coroutine error:', r)
						end
					end
					t.self:stop_append()
					return true
				end
			end
		end,
	})
end
function replicator_index:handle_stale_term(controller, state)
	state:become_follower()
	self:on_leader_auth_result(false) -- no more leader
end
function replicator_index:update_last_access()
	self.last_access = clock.get()
end
function replicator_index:update_last_appended(controller, state, entries)
	if entries and #entries > 0 then
		-- Mark any proposals as committed
		-- logger.info('entries:', #entries, first and first.index, last and last.index)
		local first, last = entries[1], entries[#entries]
		logger.debug(state:gid_for_log(), 'range commit:', first.index, last.index)
		state.proposals:range_commit(controller, first.index, last.index)

		-- Update the indexes
		self.match_idx = last.index
		self.next_idx = last.index + 1
	end
	-- still leader
	self:on_leader_auth_result(true)
end
function replicator_index:on_leader_auth_result(still_leader)
	-- logger.notice('leader status:', still_leader)
	-- TODO : invoke event to know leader status verified. eg) wait for event to avoid stale reads
end
function replicator_index:failure_cooldown(n_failure)
	clock.sleep(0.5 * n_failure)
end
function replicator_index:replicate(controller, actor, state)
	logger.report('replicate start', state:gid_for_log(), actor, self.added_idx)
	-- arguments
	local current_term, leader, 
		prev_log_idx, prev_log_term, 
		entries, leader_commit_idx
	-- response
	local ev
	local added_idx 
	local term, success, last_index
::START::
	if self.added == 0 then
		logger.debug(state:gid_for_log(), 'replicaiton to ', actor, 'has not committed yet')
		return
	elseif self.added_idx ~= 0 then
		added_idx = self.added_idx
		self.added_idx = 0
	end
	if self.failures > 0 then
		self:failure_cooldown(self.failures)
	end

	-- prepare parameters to send remote raft actor
	current_term, leader, leader_commit_idx, 
	prev_log_idx, prev_log_term, 
	entries = state:append_param_for(self)
	if not current_term then
		logger.notice('sync')
		goto SYNC
	end
	logger.notice(state:gid_for_log(), 'replicate', prev_log_idx, 'to', actor, added_idx)

	-- call AppendEntries RPC 
	-- TODO : how long timeout should be?
	--[[
	if entries then
		for idx,ent in pairs(entries) do
			for k,v in pairs(ent) do
				if type(v) == 'table' then
					for kk,vv in pairs(v) do
						logger.info('entries', idx, k, kk, vv)
					end
				else
					logger.info('entries', idx, k, v)
				end
			end
		end
	end
	--]]
	term, success, last_index = actor.timed_rpc(
									self.heartbeat_span_sec, 
									rpc.APPEND_ENTRIES,
									state:group_id(),
									current_term, leader, leader_commit_idx, 
									prev_log_idx, prev_log_term, 
									entries, added_idx)
-- print(term, success, last_index)
	-- term is updated, step down leader
	if term > current_term then
		self:handle_stale_term(controller, state)
		return true
	end

	-- update successful last access time
	self:update_last_access()

	-- Update based on success
	if success then
		-- Update our replication state
		self:update_last_appended(controller, state, entries)
		-- Clear any failures, allow pipelining
		self.failures = 0
	else
		self.next_idx = math.max(math.min(tonumber(self.next_idx)-1, tonumber(last_index)+1), 1)
		self.match_idx = self.next_idx - 1
		self.failures = self.failures + 1
		logger.warn(state:gid_for_log(), util.sprintf("raft: append_entries to %s rejected, sending older logs (next: %llu)", 256, tostring(actor), self.next_idx))
	end

::CHECK_MORE::
	-- Check if there are more logs to replicate
	-- logger.info('check more logs to replic', self.next_idx, state.wal:last_index(), leader_commit_idx)
	if (self.next_idx <= state.wal:last_index()) or (leader_commit_idx < state:last_commit_index()) then
		-- logger.info('more logs to replic', self.next_idx, state.wal:last_index())
		goto START
	else
		return
	end

	-- SYNC is used when we fail to get a log, usually because the follower
	-- is too far behind, and we must ship a snapshot down instead
::SYNC::
	local stop, err = self:sync(controller, actor, state)
	if stop then
		return true
	elseif err then
		logger.error(state:gid_for_log(), ("[ERR] raft: failed to send snapshot to %x: %s"):format(uuid.addr(actor), err))
		return
	end
	-- Check if there is more to replicate
	goto CHECK_MORE
end
-- send snap shot and sync fsm 
function replicator_index:sync(controller, actor, state)
	-- Get the snapshot path
	local path, last_snapshot_index = state.snapshot:latest_snapshot_path()
	-- no snapshot
	if not path then 
		return false 
	end
	-- create remote io and send install snapshot RPC
	local fd = rio.file(path)
	local ok, success = pcall( -- 120 == 2 min timeout 
		actor.timed_rpc, 120, rpc.INSTALL_SNAPSHOT, state:group_id(), 
		state:current_term(), state:leader(), last_snapshot_index, fd
	)
	-- remove rio
	actor_module.destroy(fd)
	if not (ok and success) then return false, success end
	
	-- Update the last contact
	self:update_last_access()

	-- Check for success
	if success then
		-- Mark any proposals are committed
		state.proposals:range_commit(controller, self.match_idx+1, last_snapshot_index)

		-- Update the indexes
		self.match_idx = last_snapshot_index
		self.next_idx = self.match_idx + 1

		-- Clear any failures
		self.failures = 0

		-- still leader
		self:on_leader_auth_result(true)
	else
		self.failures = self.failures + 1
		logger.warn(state:gid_for_log(), ("raft: install_snapshot to %x rejected"):format(uuid.addr(actor)))
	end
	return false -- keep on checking replication log is exist.
end
function replicator_index:run_heartbeat(actor, state)
	local failures = 0
	while true do
		clock.sleep(util.random_duration(self.heartbeat_span_sec))

		-- logger.info('hb', actor, state:current_term(), state:leader(), state:last_commit_index())
		ok, term, success, last_index = pcall(actor.rpc, rpc.APPEND_ENTRIES, state:group_id(), state:current_term(), state:leader())
		if (not ok) or (not success) then
			if (not ok) and (term:is('raft_not_found')) then
				-- it means target raft instance removed
				logger.warn("raft: instance removed", state:gid_for_log(), "stop heartbeat for", actor)
				break
			end
			logger.warn(state:gid_for_log(), ("raft: failed to heartbeat to %x:%x:%s"):format(uuid.addr(actor), uuid.thread_id(actor), term or "nil"))
			failures = failures + 1
			self:failure_cooldown(failures)
		else
			-- update successful last access time
			self:update_last_access()
			failures = 0

			-- still leader
			self:on_leader_auth_result(true)
		end
	end
end
ffi.metatype('luact_raft_replicator_t', replicator_mt)


-- module functions
function _M.new(controller, actor, state, activate)
	local r
	if #cache > 0 then
		r = table.remove(cache)
	else
		r = memory.alloc_fill_typed('luact_raft_replicator_t')
	end
	return r, r:start(controller, actor, state, activate)
end

return _M
