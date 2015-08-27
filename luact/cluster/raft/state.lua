local ffi = require 'ffiex.init'
local actor = require 'luact.actor'
local uuid = require 'luact.uuid'
local pbuf = require 'luact.pbuf'
local router = require 'luact.router'
local clock = require 'luact.clock'
local proposal = require 'luact.cluster.raft.proposal'
local replicator = require 'luact.cluster.raft.replicator'
local serde_common = require 'luact.serde.common'

local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local fs = require 'pulpo.fs'
local event = require 'pulpo.event'
local _M = {}

-- cdefs
ffi.cdef [[
typedef enum luact_raft_node_kind {
	NODE_FOLLOWER,
	NODE_CANDIDATE,
	NODE_LEADER,
} luact_raft_node_kind_t;
typedef enum luact_raft_system_log_kind {
	SYSLOG_ADD_REPLICA_SET,
	SYSLOG_REMOVE_REPLICA_SET,
	SYSLOG_DICTATORIAL_PROPOSE,
} luact_raft_system_log_kind_t;

typedef struct luact_raft_hardstate {
	uint64_t term;
	uint64_t last_vote_term;		// last vote term
	luact_uuid_t vote;				// voted raft actor id (not valid, not voted yet)
} luact_raft_hardstate_t;

typedef struct luact_raft_state {
	//raft state
	luact_raft_hardstate_t current;

	//raft working state
	uint64_t last_applied_idx; 		// index of highest log entry applied to state machine
	uint64_t last_commit_idx;		// index of highest log entry committed.
	uint64_t added_idx;				// log index which causes this node add to replica set. 
									// used for preventing newly added raft object from removing itself by old (already processed) log.
	luact_uuid_t vote;				// voted raft actor id (not valid, not voted yet)
	luact_uuid_t leader_id; 		// raft actor id of leader
	uint32_t stepdown_msgid;		// if ~= 0 means this node try to stepdown from raft leader
	uint8_t node_kind, padd[3];		// node type (follower/candidate/leader)
} luact_raft_state_t;
]]
local NODE_FOLLOWER = ffi.cast('luact_raft_node_kind_t', "NODE_FOLLOWER")
local NODE_CANDIDATE = ffi.cast('luact_raft_node_kind_t', "NODE_CANDIDATE")
local NODE_LEADER = ffi.cast('luact_raft_node_kind_t', "NODE_LEADER")
local SYSLOG_ADD_REPLICA_SET = ffi.cast('luact_raft_system_log_kind_t', "SYSLOG_ADD_REPLICA_SET")
local SYSLOG_REMOVE_REPLICA_SET = ffi.cast('luact_raft_system_log_kind_t', "SYSLOG_REMOVE_REPLICA_SET")
local SYSLOG_DICTATORIAL_PROPOSE = ffi.cast('luact_raft_system_log_kind_t', "SYSLOG_DICTATORIAL_PROPOSE")

-- luact_raft_hardstate_t
local raft_hardstate_index = {}
local raft_hardstate_mt = {
	__index = raft_hardstate_index
}
function raft_hardstate_index:equals(target)
	return self.term == target.term and self.last_vote_term == target.last_vote_term and self.vote == target.vote
end
function raft_hardstate_index:copy(target)
	self.term = target.term
	self.last_vote_term = target.last_vote_term
	self.vote = target.vote
end
function raft_hardstate_index:new_term()
	logger.notice('raft', 'new_term', self.term, self.term + 1)
	self.term = self.term + 1
end
function raft_hardstate_index:set_term(term)
	logger.notice('raft', 'set_term', self.term, term)
	self.term = term
end
function raft_hardstate_index:vote_for(v, term)
	if term == self.last_vote_term and uuid.valid(self.vote) then
		return uuid.equals(v, self.vote)
	end
	self.last_vote_term = self.term
	self.vote = v
	return true
end
function raft_hardstate_index:force_vote_for(v)
	self.last_vote_term = self.term
	self.vote = v
	return true
end
serde_common.register_ctype('struct', 'luact_raft_hardstate', nil, serde_common.LUACT_RAFT_HARDSTATE)
ffi.metatype('luact_raft_hardstate_t', raft_hardstate_mt)


-- luact_raft_state_conteinr_t
local raft_state_container_index = {}
local raft_state_container_mt = {
	__index = raft_state_container_index
}
function raft_state_container_index:init()
	self:restore() -- try to recover from previous persist state
	self.fsm:attach()
	self:apply_debug_opts(self.opts)
end
function raft_state_container_index:apply_debug_opts(opts)
	if opts.debug_node_kind then
		self:debug_set_node_kind(opts.debug_node_kind)
	end
	if opts.debug_leader_id then
		self.state.leader_id = opts.debug_leader_id
	end
end
function raft_state_container_index:group_id()
	return self.id
end
function raft_state_container_index:gid_for_log()
	return tostring(ffi.cast('luact_uuid_t*', self.id))
end
function raft_state_container_index:fin()
	-- TODO : recycle
	self.fsm:detach()
	self.fsm = nil
	self.controller = nil
	self.ev_log:destroy()
	self.ev_close:destroy()
	self:stop_replication()
	self.proposals:fin()
	self.wal:fin()
	self.snapshot:fin()
	if self.state then
		memory.free(self.state)
	end
end
function raft_state_container_index:quorum()
	local i = #self.replica_set
	if i <= 2 then return math.max(i, 1) end
	return math.ceil((i + 1) / 2)
end
function raft_state_container_index:write_any_logs(kind, msgid, logs)
	local q = self:quorum()
	if q >= 2 then
		local start_idx, end_idx = self.wal:write(kind, self.state.current.term, logs, msgid)
		self.proposals:add(q, start_idx, end_idx)
		self:kick_replicator()
	elseif kind then -- only system logs are able to commit by leader itself (eg. initial add nodes)
		local start_idx, end_idx = self.wal:write(kind, self.state.current.term, logs, msgid)
		-- add and commit 
		self.proposals:dictatorial_add(self.controller, start_idx, end_idx)
	else
		exception.raise('invalid', self:gid_for_log(), 'quorum too short', q)
	end
end
function raft_state_container_index:kick_replicator()
	self.ev_log:emit('add')
end	
function raft_state_container_index:write_logs(msgid, logs)
	self:write_any_logs(nil, msgid, logs)
end
function raft_state_container_index:write_log(msgid, log)
	self:write_any_logs(nil, msgid, {log})
end
function raft_state_container_index:dictatorial_write_logs(msgid, logs)
	self:write_any_logs(SYSLOG_DICTATORIAL_PROPOSE, msgid, logs)
end
function raft_state_container_index:write_syslog(kind, msgid, log)
	self:write_any_logs(kind, msgid, {log})
end
function raft_state_container_index:read_state()
	local st = self.wal:read_state()
	if st then
		self.state.current = st
	end
	return st ~= nil
end
function raft_state_container_index:set_last_commit_index(idx)
	self.state.last_commit_idx = idx
end	
function raft_state_container_index:new_term()
	self.state.current:new_term()
	uuid.invalidate(self.state.leader_id)
	self.wal:write_state(self.state.current)
end
function raft_state_container_index:set_term(term)
	self.state.current:set_term(term)
end
function raft_state_container_index:vote_for_self()
	self.state.current:force_vote_for(self.controller.manager)
	self.wal:write_state(self.state.current)
end
function raft_state_container_index:vote_for(v, term)
	if self.state.current:vote_for(v, term) then
		self.wal:write_state(self.state.current)
		return true
	end
	return false
end
function raft_state_container_index:update_last_removal_index(idx)
	self.state.current.last_removal_idx = idx
	self.wal:write_state(self.state.current)	
end
function raft_state_container_index:set_leader(leader_id)
	-- logger.info('set leader', leader_id)
	local changed, change_self
	if leader_id then
		if not uuid.equals(self.state.leader_id, leader_id) then
			changed = true
			if uuid.equals(self.controller.manager, leader_id) then
				change_self = true
			end
		end		
		self.state.leader_id = leader_id
	else
		if uuid.valid(self.state.leader_id) then
			changed = true
			if uuid.equals(self.controller.manager, self.state.leader_id) then
				change_self = true
			end
		end
		uuid.invalidate(self.state.leader_id)
	end
	if changed then
		if self.state.stepdown_msgid ~= 0 and uuid.valid(self.state.leader_id) and 
			(not uuid.equals(self.state.leader_id, self.controller.manager)) then
			local msgid = tonumber(self.state.stepdown_msgid)
			self.state.stepdown_msgid = 0
			router.respond_by_msgid(msgid, true, true)
		end
		self.fsm:change_replica_set('change_leader', change_self, self.state.leader_id, self.replica_set)
	end
end
-- only for debug
function raft_state_container_index:debug_set_node_kind(k)
	self.state.node_kind = ffi.cast('luact_raft_node_kind_t', ("NODE_"..k):upper())
end
function raft_state_container_index:become_leader()
	if not self:is_candidate() then
		exception.raise('raft', self:gid_for_log(), 'invalid state change', 'leader', self.state.node_kind)
	end
	self.state.node_kind = NODE_LEADER
	self:set_leader(self.controller.manager)
	self:start_replication(true)
end
function raft_state_container_index:become_candidate()
	if self.state.node_kind ~= NODE_FOLLOWER then
		exception.raise('raft', self:gid_for_log(), 'invalid state change', 'candidate', self.state.node_kind)
	end
	self.state.node_kind = NODE_CANDIDATE
	self:set_leader(nil)
	self.controller:start_election()
end
function raft_state_container_index:become_follower(stepdown_msgid)
	if self:is_leader() then
		self:stop_replication()
		if stepdown_msgid then
			self.state.stepdown_msgid = stepdown_msgid
			self.controller:reset_stepdown_timeout()
		end
	end
	self.state.node_kind = NODE_FOLLOWER
	self:set_leader(nil)
end
-- nodes are list of luact_uuid_t
function raft_state_container_index:stop_replicator(target_actor)
	local machine, thread = uuid.machine_id(target_actor), uuid.thread_id(target_actor)
	local m = self.replicators[machine]
	if m and m[thread] then
		local r = m[thread]
		self.ev_log:emit('stop', r)
		r:fin()
		m[thread] = nil
	end
end
function raft_state_container_index:stop_replication()
	-- stop all main replication tentacle
	self.ev_log:emit('stop')
	-- even if no leader, try to stop replicaiton
	for machine, reps in pairs(self.replicators) do
		for k,v in pairs(reps) do
			if not self:is_leader() then
				logger.error('bug: no-leader node have valid replicator', self:gid_for_log(), machine, k)
			end
			logger.debug('stop replicator', self:gid_for_log(), machine, k)
			v:fin()
			reps[k] = nil
		end
	end
end
function raft_state_container_index:start_replication(activate, replica_set)
	local leader_id = self:leader()
	replica_set = replica_set or self.replica_set
	for i = 1,#replica_set do
		id = replica_set[i]
		if not uuid.equals(leader_id, id) then
			local machine, thread = uuid.machine_id(id), uuid.thread_id(id)
			local m = self.replicators[machine]
			if not m then
				m = {}
				self.replicators[machine] = m
			end
			if not m[thread] then
				m[thread] = replicator.new(self.controller, id, self, activate)
				-- in here, replicator only do heartbeat, never involve replication
				-- until previous replica agrees this operation.
				-- heartbeat is necessary because in our use case, 
				-- typically need to prevent newly added node from causing election timeout.
				if activate then
					logger.info('start replicator', self:gid_for_log(), id)
				else
					logger.info('add replicator', self:gid_for_log(), id)
				end
			end
		end
	end
	self:kick_replicator()
end
-- TODO : arbitrary replica set change should do with sequencial flow like
-- add_replica => (wait for completion) => remove_replica 
-- like "6 Cluster membership changes" of original raft paper.

-- prevent immediate change of replica_set allows network partitioned minority to execute un-authorized write,
-- newly added replicator only starts replication after this operation agreed by previous replica set.
function raft_state_container_index:add_replica_set(msgid, replica_set, applied_idx)
	local self_actor = self.controller.manager
	if applied_idx then
		if applied_idx <= self.initial_replica_set_ver_idx then
			logger.info('older replica_set log than current replica_set version', applied_idx, self.initial_replica_set_ver_idx)
			return
		end
		local add_self, changed
		for i = 1,#replica_set do
			local found
			for j = 1,#self.replica_set do
				if uuid.equals(self.replica_set[j], replica_set[i]) then
					if uuid.equals(self_actor, self.replica_set[j]) then
						add_self = true
					end
					found = j
					break
				end
			end
			if not found then
				table.insert(self.replica_set, replica_set[i])
				changed = true
			end
		end
		if self:is_leader() then
			-- only leader need to setup replication
			local leader_id = self:leader()
			for i = 1,#replica_set do
				id = replica_set[i]
				if not uuid.equals(leader_id, id) then
					local machine, thread = uuid.addr(id), uuid.thread_id(id)
					local m = self.replicators[machine]
					if m and m[thread] then
						-- from here, this replicator actually involve replication
						m[thread]:start_append(applied_idx)
						logger.debug('start replicator', self:gid_for_log(), id, applied_idx)
					end
				end
			end
			self:kick_replicator()
		end
		if changed then
			self.fsm:change_replica_set('add', add_self, self.state.leader_id, self.replica_set)
		end
	elseif self:is_leader() then 
		if type(replica_set) ~= 'table' then
			replica_set = {replica_set}
		end
		self:start_replication(false, replica_set)
		self:write_syslog(SYSLOG_ADD_REPLICA_SET, msgid, replica_set)
	end
end
-- prevent immediate change of replica_set allows network partitioned minority to execute un-authorized write,
-- actual replica set only changed if previous replica set commit this proposal.
function raft_state_container_index:remove_replica_set(msgid, replica_set, applied_idx)
	local self_actor = self.controller.manager
	if applied_idx then
		if applied_idx <= self.initial_replica_set_ver_idx then
			logger.info('older replica_set log than current replica_set version', applied_idx, self.initial_replica_set_ver_idx)
			return
		end
		local remove_self, changed
		local fsm = self.fsm
		for i = 1,#replica_set do
			local found
			for j = 1,#self.replica_set do
				if uuid.equals(self.replica_set[j], replica_set[i]) then
					if uuid.equals(self_actor, self.replica_set[j]) then
						remove_self = true
					end
					found = j
					break
				end
			end
			if found then
				-- we cannot stop replicator here because after that replicator will notify
				-- 'applied' to removed replica, too.
				table.remove(self.replica_set, found)
				changed = true
			end
		end
		if self:is_leader() then
			-- actual replicator removal is occured at following order
			-- 1. replicator knows replicate target is gone (by error:is('raft_not_found'))
			-- 2. call state:stop_replicator
			-- 3. stop_replicator emit stop event to target replicator
			-- 4. target repliactor cancel all coroutines, stop_append().
			-- TODO : we need to care about re-addition of the node before above sequence occurs (usually occurs within 100msec)? 
		elseif remove_self then
			self.controller:destroy()
		end
		if changed then
			-- TODO : for correctness, should we move this to stop_replicator?
			fsm:change_replica_set('remove', remove_self, self.state.leader_id, self.replica_set)
		end
	elseif self:is_leader() then
		if type(replica_set) ~= 'table' then
			replica_set = {replica_set}
		end
		for i = 1,#replica_set do
			if uuid.equals(replica_set[i], self_actor) then
				logger.warn('get a grip!! you are leader and try to remove yourself!!', self:gid_for_log())
				table.remove(replica_set, i)
				break
			end
		end
		-- append log to replicate configuration change
		self:write_syslog(SYSLOG_REMOVE_REPLICA_SET, msgid, replica_set)
	end
end
function raft_state_container_index:request_routing_id(timeout)
::RETRY::
	if type(timeout) ~= 'number' then
		logger.error('no number')
	end
	if self:is_leader() then 
		return nil, timeout
	elseif uuid.valid(self.state.leader_id) then
		return self.state.leader_id, timeout
	end
	if timeout > 0 then
		local start = clock.get()
		clock.sleep(0.5)
		-- logger.info('wait for leader elected:', timeout, timeout - (clock.get() - start))
		timeout = timeout - (clock.get() - start)
		goto RETRY
	end
	exception.raise('runtime', self:gid_for_log(), 'raft state', "no leader but don't know who is leader")
end
function raft_state_container_index:leader()
	return self.state.leader_id
end
function raft_state_container_index:is_leader()
	return self.state.node_kind == NODE_LEADER
end
function raft_state_container_index:is_follower()
	return self.state.node_kind == NODE_FOLLOWER
end
function raft_state_container_index:is_candidate()
	return self.state.node_kind == NODE_CANDIDATE
end
function raft_state_container_index:current_term()
	return self.state.current.term
end
function raft_state_container_index:last_commit_index()
	return self.state.last_commit_idx
end
function raft_state_container_index:last_applied_index()
	return self.state.last_applied_idx
end
function raft_state_container_index:last_index()
	return self.wal:last_index()
end
function raft_state_container_index:set_added_index(added_idx)
	self.state.added_idx = added_idx
end
function raft_state_container_index:snapshot_if_needed()
	if (self.state.last_applied_idx - self.snapshot:last_index()) >= self.opts.logsize_snapshot_threshold then
		-- print('if_needed', self.state.last_applied_idx, self.snapshot:last_index(), self.opts.logsize_snapshot_threshold)
		logger.debug('snapshot', self:gid_for_log(), self.state.last_applied_idx)
		self:write_snapshot()
	end
end
function raft_state_container_index:has_enough_nodes_for_election()
	if self.opts.debug_dictatorial then
	 	return true
	end
	-- at least 2 replica needed (because total quorum need to be >= 3)
	return #self.replica_set > 1
end
-- after replication to majority success, apply called
function raft_state_container_index:committed(log_idx)
	if (self:last_commit_index() > 0) and (log_idx - self:last_commit_index()) ~= 1 then
		logger.warn('raft', self:gid_for_log(), 'commit log idx leap (may be previous leader stale)', self:last_commit_index(), log_idx)
	end
	self:set_last_commit_index(log_idx)
end
function raft_state_container_index:applied(log_idx)
	if (self.state.last_applied_idx > 0) and (log_idx - self.state.last_applied_idx) ~= 1 then
		exception.raise('fatal', self:gid_for_log(), 'invalid committed log idx', log_idx, self.state.last_applied_idx)
	end	
	self.state.last_applied_idx = log_idx
	self:snapshot_if_needed()
end
function raft_state_container_index:apply_result(log, ok, ...)
	if ok then
		self:applied(log.index)
	end
	return ok, ...
end
function raft_state_container_index:do_apply(log, ...)
	return self:apply_result(log, pcall(...))
end
function raft_state_container_index:apply(log)
	if not log.kind then
		-- apply to fsm
		return self:do_apply(log, self.fsm.apply, self.fsm, log.log)
	elseif log.kind == SYSLOG_DICTATORIAL_PROPOSE then
		return self:do_apply(log, self.fsm.apply, self.fsm, log.log)		
	elseif log.kind == SYSLOG_ADD_REPLICA_SET then
		return self:do_apply(log, self.add_replica_set, self, nil, log.log, log.index)
	elseif log.kind == SYSLOG_REMOVE_REPLICA_SET then
		return self:do_apply(log, self.remove_replica_set, self, nil, log.log, log.index)
	else
		logger.warn('invalid raft system log committed', self:gid_for_log(), log.kind)
	end
end
function raft_state_container_index:restore()
	if not self.wal:can_restore() then
		logger.warn('persistent data is not for current fsm', self:gid_for_log())
		self.wal:create_metadata_entry()
		return 
	end
	local r = self:read_state()
	local idx,hd = self.snapshot:restore(self.fsm)
	if hd then 
		self:read_snapshot_header(hd)
		-- we have unapplied WAL but its unclear all contents of it, is committed.
		-- so, we wait for next commit (whether if this node become leader or follower)
		if not r then
			local i, t = self.snapshot:last_index_and_term()
			-- restore state from snapshot
			self.state.current.term = t
			-- start with no vote
		end
	end
end
function raft_state_container_index:restore_from_snapshot(ssrb)
	local idx,hd = self.snapshot:restore(self.fsm, ssrb)
	if hd then 
		self:read_snapshot_header(hd)
		-- we have unapplied WAL but its unclear all contents of it, is committed.
		-- so, we wait for next commit (whether if this node become leader or follower)
		-- restore state from snapshot
		local i, t = self.snapshot:last_index_and_term()
		self.state.current.term = t
		-- start with no vote
	end
end
function raft_state_container_index:write_snapshot_header(hd)
	if hd.n_replica < #self.replica_set then
		hd = hd:realloc(#self.replica_set)
	end
	hd.term = self:current_term()
	hd.index = self.state.last_applied_idx
	hd.n_replica = #self.replica_set
	for i=1,#self.replica_set do
		hd.replicas[i - 1] = self.replica_set[i]
	end
	return hd	
end
function raft_state_container_index:read_snapshot_header(hd)
	-- should use latest state (by self:read_state()) for term
	-- self.state.current.term = hd.term
	self.state.last_applied_idx = hd.index
	self.initial_replica_set_ver_idx = hd.index
	for i=1,hd.n_replica do
		table.insert(self.replica_set, hd.replicas[i - 1])
	end	
end
function raft_state_container_index:write_snapshot()
	local last_snapshot_idx = self.snapshot:write(self.fsm, self)
	self.wal:compaction(last_snapshot_idx)
	self.snapshot:trim(self.opts.snapshot_file_preserve_num)
end
function raft_state_container_index:append_param_for(replicator)
	local prev_log_idx, prev_log_term
	if replicator.next_idx == 1 then
		prev_log_idx, prev_log_term = 0, 0
	elseif (replicator.next_idx - 1) == self.snapshot.writer.last_snapshot_idx then
		prev_log_idx, prev_log_term = self.snapshot:last_index_and_term()
	else
		local log = self.wal:at(replicator.next_idx - 1)
		-- TODO : this actually happens because quorum of this index is already satisfied, 
		-- log may be compacted already. compaction margin make some help, but not perfect.
		if not log then
			return false, exception.new('raft', self:gid_for_log(), 'invalid', 'next_index', replicator.next_idx)
		end
		prev_log_idx = log.index
		prev_log_term = log.term
	end 
	local entries = self.wal:logs_from(prev_log_idx + 1)
	assert(prev_log_idx and prev_log_term, "error: prev index/term should be non-nil")
	return 
		self:current_term(), 
		self:leader(),
		self:last_commit_index(),
		prev_log_idx,
		prev_log_term,
		entries
end


-- module function
local function compute_initial_replica_set(opts)
	return opts.replica_set or {}
end
function _M.new(id, fsm, wal, snapshot, opts)
	local r = setmetatable({
		id = id,
		state = memory.alloc_fill_typed('luact_raft_state_t'), 
		proposals = proposal.new(wal, opts.initial_proposal_size), 
		ev_log = event.new(), 
		ev_close = event.new(),
		wal = wal, -- logs index is self.offset_idx to self.offset_idx + #logs - 1
		replicators = {}, -- replicators to replicate logs. initialized and sync'ed by replica_set
		-- replica_set is array of actor (luact_uuid_t) which represents current replica set, include leader replica.
		replica_set = compute_initial_replica_set(opts), 
		-- initial_replica_set_ver_idx is applied log index of raft instance which provide opts.replica_set.
		-- this is used for skipping (add|remove)_replica_set log which is already outdated.
		initial_replica_set_ver_idx = opts.replica_set and opts.initial_replica_set_ver_idx or 0, 
		fsm = fsm, -- finite state machine (FSM) which this raft group maintains.
		snapshot = snapshot, -- snapshotter of fsm and raft hardstate.
		opts = opts,
	}, raft_state_container_mt)
	r:init()
	return r
end

return _M
