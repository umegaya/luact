local ffi = require 'ffiex.init'
local actor = require 'luact.actor'
local uuid = require 'luact.uuid'
local pbuf = require 'luact.pbuf'
local proposal = require 'luact.cluster.raft.proposal'
local replicator = require 'luact.cluster.raft.replicator'

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
	luact_uuid_t leader_id; 		// raft actor id of leader
	uint8_t node_kind, padd[3];		// node type (follower/candidate/leader)
} luact_raft_state_t;
]]
local NODE_FOLLOWER = ffi.cast('luact_raft_node_kind_t', "NODE_FOLLOWER")
local NODE_CANDIDATE = ffi.cast('luact_raft_node_kind_t', "NODE_CANDIDATE")
local NODE_LEADER = ffi.cast('luact_raft_node_kind_t', "NODE_LEADER")
local SYSLOG_ADD_REPLICA_SET = ffi.cast('luact_raft_system_log_kind_t', "SYSLOG_ADD_REPLICA_SET")
local SYSLOG_REMOVE_REPLICA_SET = ffi.cast('luact_raft_system_log_kind_t', "SYSLOG_REMOVE_REPLICA_SET")


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
	logger.notice('raft', 'new term', self.term, self.term + 1)
	self.term = self.term + 1
end
function raft_hardstate_index:set_term(term)
	logger.notice('raft', 'set term', self.term, term)
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
function raft_state_container_index:fin()
	-- TODO : recycle
	self.actor_body = nil
	self.ev_log:destroy()
	self.ev_close:destroy()
	self:stop_replication()
	self.proposals:fin()
	self.wal:fin()
	self.snapshot:fin()
	self.fsm:detach()
	if self.state then
		memory.free(self.state)
	end
end
function raft_state_container_index:quorum()
	local i = 0
	for k,v in pairs(self.replicators) do
		for _,rep in pairs(v) do
			i = i + 1
		end
	end
	if i <= 2 then return i end
	return math.ceil((i + 1) / 2)
end
function raft_state_container_index:write_any_logs(kind, msgid, logs)
	local q = self:quorum()
	if q < 2 and (not kind) then
		exception.raise('invalid', 'quorum too short', q)
	end
	local start_idx, end_idx = self.wal:write(kind, self.state.current.term, logs, msgid)
	self.proposals:add(q, start_idx, end_idx)
	self:kick_replicator()
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
function raft_state_container_index:vote_for(v, term)
	if self.state.current:vote_for(v, term) then
		self.wal:write_state(self.state.current)
		return true
	end
	return false
end
function raft_state_container_index:set_leader(leader_id)
	-- logger.info('set leader', leader_id)
	self.state.leader_id = leader_id
end
-- only for debug
function raft_state_container_index:debug_set_node_kind(k)
	self.state.node_kind = ffi.cast('luact_raft_node_kind_t', ("NODE_"..k):upper())
end
function raft_state_container_index:become_leader()
	if not self:is_candidate() then
		exception.raise('raft', 'invalid state change', 'leader', self.state.node_kind)
	end
	self.state.node_kind = NODE_LEADER
	self:set_leader(actor.of(self.actor_body))
	-- run replicator (and write initial log)
	self:add_replica_set(self.replica_set)
end
function raft_state_container_index:become_candidate()
	if self.state.node_kind ~= NODE_FOLLOWER then
		exception.raise('raft', 'invalid state change', 'candidate', self.state.node_kind)
	end
	self.state.node_kind = NODE_CANDIDATE
	self.actor_body:start_election()
end
function raft_state_container_index:become_follower()
	if self:is_leader() then
		self:stop_replication()
	end
	self.state.node_kind = NODE_FOLLOWER
end
-- nodes are list of luact_uuid_t
function raft_state_container_index:stop_replication()
	-- even if no leader, try to stop replicaiton
	for machine, reps in pairs(self.replicators) do
		for k,v in pairs(reps) do
			if not self:is_leader() then
				logger.error('bug: no-leader node have valid replicator')
			end
			v:fin()
			reps[k] = nil
		end
	end
end
-- replica_set are list of luact_uuid_t
function raft_state_container_index:set_replica_transition(on)
	self.replica_transition = (on and 1 or 0)
end
-- TODO : arbitrary replica set change should do with sequencial flow like
-- add_replica => (wait for completion) => remove_replica 
-- like 6 Cluster membership changes or original raft paper.
function raft_state_container_index:add_replica_set(msgid, replica_set)
	if type(replica_set) ~= 'table' then
		replica_set = {replica_set}
	end
	local changed
	for i = 1,#replica_set do
		local found
		for j = 1,#self.replica_set do
			if uuid.equals(self.replica_set[j], replica_set[i]) then
				found = j
			end
		end
		if not found then
			table.insert(self.replica_set, replica_set[i])
			changed = true
		end
	end
	if changed and self:is_leader() then 
		-- TODO : make following transactional
		-- only leader need to setup replication
		local leader_id = self:leader()
		for i = 1,#replica_set do
			id = replica_set[i]
			if not uuid.equals(leader_id, id) then
				local machine, thread = uuid.addr(id), uuid.thread_id(id)
				local m = self.replicators[machine]
				if not m then
					m = {}
					self.replicators[machine] = m
				end
				if not m[thread] then
					-- id is act like actor
					m[thread] = replicator.new(leader_id, id, self)
				end
			end
		end
		-- append log to replicate configuration change
		self:write_syslog(SYSLOG_ADD_REPLICA_SET, msgid, replica_set)
	end
end
-- replica_set are list of luact_uuid_t
function raft_state_container_index:remove_replica_set(msgid, replica_set)
	if type(replica_set) ~= 'table' then
		replica_set = {replica_set}
	end
	local changed
	for i = 1,#replica_set do
		local found
		for j = 1,#self.replica_set do
			if uuid.equals(self.replica_set[j], replica_set[i]) then
				found = j
			end
		end
		if found then
			table.remove(self.replica_set, j)
			changed = true
		end
	end
	if changed and self:is_leader() then
		-- TODO : make following transactional
		-- only leader need to setup replication
		for i = 1,#replica_set do
			id = replica_set[i]
			local machine, thread = uuid.addr(id), uuid.thread_id(id)
			local m = self.replicators[machine]
			if m and m[thread] then
				m[thread]:fin()
				m[thread] = nil
			end
		end
		-- append log to replicate configuration change
		self:write_syslog(SYSLOG_REMOVE_REPLICA_SET, msgid, replica_set)
	end
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
function raft_state_container_index:snapshot_if_needed()
	if (self.state.last_applied_idx - self.snapshot:last_index()) >= self.opts.logsize_snapshot_threshold then
		-- print('if_needed', self.state.last_applied_idx, self.snapshot:last_index(), self.opts.logsize_snapshot_threshold)
		logger.info('snapshot', self.state.last_applied_idx)
		self:write_snapshot()
	end
end
-- after replication to majority success, apply called
function raft_state_container_index:committed(log_idx)
	if (self:last_commit_index() > 0) and (log_idx - self:last_commit_index()) ~= 1 then
		exception.raise('fatal', 'invalid log idx', self:last_commit_index(), log_idx)
	end
	self:set_last_commit_index(log_idx)
end
function raft_state_container_index:applied(log_idx)
	if (self.state.last_applied_idx > 0) and (log_idx - self.state.last_applied_idx) ~= 1 then
		exception.raise('fatal', 'invalid committed log idx', log_idx, self.state.last_applied_idx)
	end	
	self.state.last_applied_idx = log_idx
	self:snapshot_if_needed()
end
function raft_state_container_index:apply(log)
	local ok, r
	if not log.kind then
		-- apply to fsm
		ok, r = pcall(self.fsm.apply, self.fsm, log.log)
	elseif log.kind == SYSLOG_ADD_REPLICA_SET then
		ok, r = pcall(self.add_replica_set, self, nil, log.log)
	elseif log.kind == SYSLOG_REMOVE_REPLICA_SET then
		ok, r = pcall(self.remove_replica_set, self, nil, log.log)
	else
		logger.warn('invalid raft system log committed', log.kind)
	end
	if ok then
		self:applied(log.index)
	end
	return ok, r
end
function raft_state_container_index:restore()
	if not self.wal:can_restore() then
		logger.warn('persistent data is not for current fsm')
		self.wal:create_metadata_entry()
		return 
	end
	local r = self:read_state()
	if r then
		logger.report('read state:', self.state.current.term)
		if self.state.current.term > 30 then
			exception.raise('invalid', 'term', self.state.current.term)
		end
	end
	local idx,hd = self.snapshot:restore(self.fsm)
	if hd then 
		self:read_snapshot_header(hd)
		-- we have unapplied WAL but its unclear all contents of it, is committed.
		-- so, we wait for next commit (whether if this node become leader or follower)
		if not r then
			-- restore state from snapshot
			self.state.current.term = self.snapshot:last_index()
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
		self.state.current.term = self.snapshot:last_index()
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
	hd.replica_transition = self.replica_transition
	for i=1,#self.replica_set do
		hd.replicas[i - 1] = self.replica_set[i]
	end
	return hd	
end
function raft_state_container_index:read_snapshot_header(hd)
	-- should use latest state (by self:read_state()) for term
	-- self.state.current.term = hd.term
	self.state.last_applied_idx = hd.index
	self.replica_transition = 0 -- hd.replica_transition
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
		local log = self.wal:at(replicator.next_idx)
		-- TODO : this actually happens because quorum of this index is already satisfied, 
		-- log may be compacted already. compaction margin make some help, but not perfect.
		if not log then
			exception.raise('raft', 'invalid', 'next_index', replicator.next_idx)
		end
		prev_log_idx = log.index
		prev_log_term = log.term
	end 
	local entries = self.wal:logs_from(prev_log_idx + 1)
	-- logger.info('append_param_for:', prev_log_idx, #entries)
	if not entries then return false end
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
function _M.new(fsm, wal, snapshot, opts)
	local r = setmetatable({
		state = memory.alloc_fill_typed('luact_raft_state_t'), 
		proposals = proposal.new(wal, opts.initial_proposal_size), 
		ev_log = event.new(), 
		ev_close = event.new(),
		wal = wal, -- logs index is self.offset_idx to self.offset_idx + #logs - 1
		replicators = {}, -- replicators to replicate logs. initialized and sync'ed by replica_set
		replica_transition = 0, -- is replica_set transition ongoing?
		replica_set = {}, -- current replica set. array of actor (luact_uuid_t)
		fsm = fsm, 
		snapshot = snapshot,
		opts = opts,
	}, raft_state_container_mt)
	r:init()
	return r
end

return _M
