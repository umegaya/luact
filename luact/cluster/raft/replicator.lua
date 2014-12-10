local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
local tentacle = require 'pulpo.tentacle'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'
local event = require 'pulpo.event'

local clock = require 'luact.clock'
local uuid = require 'luact.uuid'

local _M = {}

ffi.cdef[[
typedef struct luact_raft_replicator {
	uint64_t next_idx, match_idx;
	uint8_t alive, failures, padd[2];
	double last_access, heartbeat_span_sec;
} luact_raft_replicator_t;
]]

local replicator_index = {}
local replicator_mt = {
	__index = replicator_index
}
function replicator_index:init(state)
	self.next_idx = state.wal:last_index() + 1
	self.match_idx = 0
	self.alive = 1
	self.heartbeat_span_sec = state.opts.heartbeat_timeout_sec / 10
end
function replicator_index:fin()
	self.alive = 0
end
function replicator_index:start(actor, state)
	self:init(state)
	tentacle(self.heartbeat, self, actor, state)
	tentacle(self.run, self, actor, state)
end
function replicator_index:run(actor, state)
	event.select({
		self = self, 
		actor = actor, 
		state = state, 
		[state.ev_close] = function (t)
			t.self.alive = 0
			return true
		end,
		[state.ev_log] = function (t, tp, ...)
			if not t.running then
				t.running = true
				local ok, r = pcall(t.self.replicate, t.self, t.actor, t.state)
				if not ok then
					logger.error('raft', 'replicate', r)
				end
				return ok and r
			end
		end,
	})
end
function replicator_index:handle_stale_term()
	state:become_follower()
	self:on_leader_auth_result(false) -- no more leader
end
function replicator_index:update_last_access()
	self.last_access = clock.get()
end
function replicator_index:update_last_appended(actor, state, entries)
	-- Mark any proposals as committed
	local first, last = entries[1], entries[#entries]
	state.proposals:range_commit(actor, first.index, last.index)

	-- Update the indexes
	self.match_idx = last.index
	self.next_idx = last.index + 1

	-- still leader
	self:on_leader_auth_result(true)
end
function replicator_index:on_leader_auth_result(still_leader)
	logger.notice('leader status:', still_leader)
	-- TODO : invoke event to know leader status verified. eg) wait for event to avoid stale reads
end
function replicator_index:failure_cooldown(n_failure)
	clock.sleep(0.5 * n_failure)
end
function replicator_index:replicate(actor, state)
	-- arguments
	local current_term, leader, 
		prev_log_idx, prev_log_term, 
		entries, leader_commit_idx
	-- response
	local term, success, last_index
::START::
	if self.failures > 0 then
		self:failure_cooldown(self.failure)
	end
	if self.alive == 0 then return true end

	-- prepare parameters to send remote raft actor
	current_term, leader, 
	prev_log_idx, prev_log_term, 
	entries, leader_commit_idx = state:append_param_for(self)
	if not current_term then
		goto SYNC
	end

	-- call AppendEntries RPC 
	-- TODO : how long timeout should be?
	term, success, last_index = actor:timed_append_entries(
												self.heartbeat_span_sec, 
												current_term, leader, 
												prev_log_idx, prev_log_term, 
												entries, leader_commit_idx)
-- print(term, success, last_index)
	if self.alive == 0 then return true end
	-- term is updated, step down leader
	if term > current_term then
		self:handle_stale_term(actor, state)
		return true
	end

	-- update successful last access time
	self:update_last_access()

	-- Update based on success
	if success then
		-- Update our replication state
		self:update_last_appended(actor, state, entries)
		-- Clear any failures, allow pipelining
		self.failures = 0
	else
		s.next_idx = math.max(math.min(self.next_idx-1, last_index+1), 1)
		self.match_idx = s.next_idx - 1
		self.failures = self.failures + 1
		logger.warn(("raft: AppendEntries to %x rejected, sending older logs (next: %d)"):format(uuid.addr(actor), s.next_idx))
	end

::CHECK_MORE::
	-- Check if there are more logs to replicate
	if self.next_idx <= state.wal:last_index() then
		goto START
	else
		return
	end

	-- SYNC is used when we fail to get a log, usually because the follower
	-- is too far behind, and we must ship a snapshot down instead
::SYNC::
	local stop, err = self:sync(actor, state)
	if stop then
		return true
	elseif err then
		logger.error(("[ERR] raft: Failed to send snapshot to %x: %s"):format(uuid.addr(actor), err))
		return
	end
	-- Check if there is more to replicate
	goto CHECK_MORE
end
-- send snap shot and sync fsm 
function replicator_index:sync(actor, state)
	-- Get the snapshots
	local rb, serde, size = state.snapshot:latest_snapshot()
	if not rb then
		return false, exception.new('not_found', 'snapshot')
	end
	local obj, err = serde:unpack(rb)
	if err then
		return false, exception.new('invalid', 'snapshot', err)
	end

	local term, success, last_index = actor:install_snapshot(
		state:current_term(), state:leader(), obj.index, obj.term, obj:to_table(), size
	)
	if self.alive == 0 then return true end
	-- TODO : how to pass snapshot *body*?
	success = false

	if self.alive == 0 then return true end
	-- Check for a newer term, stop running
	if term > state:current_term() then
		self:handle_stale_term(actor, state)
		return false
	end

	-- Update the last contact
	self:update_last_access()

	-- Check for success
	if success then
		-- Mark any proposals are committed
		state.proposals:range_commit(self.match_idx+1, obj.index)

		-- Update the indexes
		self.match_idx = obj.index
		self.next_idx = s.match_idx + 1

		-- Clear any failures
		self.failures = 0

		-- still leader
		self:on_leader_auth_result(true)
	else
		self.failures = self.failures + 1
		logger.warn(("raft: InstallSnapshot to %x rejected"):format(uuid.addr(actor)))
	end
	return false
end
function replicator_index:heartbeat(actor, state)
	local failures = 0
	while self.alive ~= 0 do
		clock.sleep(self.heartbeat_span_sec)

		ok, term, success, last_index = pcall(actor.append_entries, actor, state:current_term(), state:leader())
		if (not ok) or (not success) then
			logger.error(("raft: Failed to heartbeat to %x:%x"):format(uuid.addr(actor), uuid.thread_id(actor)))
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
function _M.new(actor, state)
	local r = memory.alloc_fill_typed('luact_raft_replicator_t')
	r:start(actor, state)
	return r
end

return _M
