local luact = require 'luact.init'
local uuid = require 'luact.uuid'
local clock = require 'luact.clock'
local serde = require 'luact.serde'
local router = require 'luact.router'
local actor = require 'luact.actor'

local state = require 'luact.cluster.raft.state'
local wal = require 'luact.cluster.raft.wal'
local snapshot = require 'luact.cluster.raft.snapshot'

local pulpo = require 'pulpo.init'
local event = require 'pulpo.event'
local util = require 'pulpo.util'
local nevent = util -- for GO style select
local socket = require 'pulpo.socket'
local tentacle = require 'pulpo.tentacle'
local fs = require 'pulpo.fs'

local exception = require 'pulpo.exception'
exception.define('raft')
exception.define('raft_invalid', { recoverable = true })
exception.define('raft_not_found', { recoverable = true })
exception.define('raft_invalid_rpc', { recoverable = true })

local _M = require 'luact.cluster.raft.rpc'
local raft_actor
local raftmap = {}
local raft_bodymap = {}

-- tick interval
local tick_intval_sec = 0.05

-- raft rpc type
local APPEND_ENTRIES = _M.APPEND_ENTRIES
local REQUEST_VOTE = _M.REQUEST_VOTE
local INSTALL_SNAPSHOT = _M.INSTALL_SNAPSHOT

local INTERNAL_ACCEPTED = _M.INTERNAL_ACCEPTED
local INTERNAL_PROPOSE = _M.INTERNAL_PROPOSE
local INTERNAL_ADD_REPLICA_SET = _M.INTERNAL_ADD_REPLICA_SET
local INTERNAL_REMOVE_REPLICA_SET = _M.INTERNAL_REMOVE_REPLICA_SET

-- raft object methods
local raft_index = {}
local raft_mt = {
	__index = raft_index
}
function raft_index:init()
end
function raft_index:fin()
	self.state:fin()
end	
function raft_index:group_id()
	return self.id
end
function raft_index:gid_for_log()
	return tostring(ffi.cast('luact_uuid_t*', self.id))
end
function raft_index:destroy()
	local a = self.manager
	logger.notice(('node %x:%d (%s) removed from raft group "%s"'):format(
		uuid.addr(a), uuid.thread_id(a), a, self:gid_for_log()))
	raft_bodymap[self.id] = nil
	self.alive = nil
	if self.main_thread then
		tentacle.cancel(self.main_thread)
	end
	if self.election_thread then
		tentacle.cancel(self.election_thread)
	end
	self:fin()
end
function raft_index:check_election_timeout()
	return clock.get() >= self.timeout_limit
end
function raft_index:reset_timeout(dur)
	dur = dur or util.random_duration(self.opts.election_timeout_sec)
	self.timeout_limit = clock.get() + dur
end
function raft_index:reset_stepdown_timeout()
	self:reset_timeout(math.max(5, self.opts.election_timeout_sec * 5))
end
function raft_index:initial_set_timeout()
	if self.opts.debug_leader_uuid and (not uuid.equals(_M.manager_actor(), self.opts.debug_leader_uuid)) then
		self:reset_stepdown_timeout()
		return
	end
	self:reset_timeout()
end
function raft_index:tick()
	if self.state:is_follower() then
		-- logger.info('follower election timeout check', self.timeout_limit - clock.get())
		if self:check_election_timeout() then
			logger.info('follower election timeout', self:gid_for_log(), self.timeout_limit, clock.get())
			if self.state:has_enough_nodes_for_election() then
				-- if election timeout (and has enough node), become candidate
				self.state:become_candidate()
			else
				logger.warn('election timeout but # of nodes not enough', self:gid_for_log(), 
					tostring(self.state.initial_node), #self.state.replica_set)
				self:reset_timeout() -- wait for receiving replica set from leader
			end
		end
	end
end
function raft_index:start()
	self.main_thread = tentacle(self.launch, self)
end
function raft_index:launch()
	pcall(self.run, self)
	self.main_thread = nil
end
function raft_index:run()
	self.state:become_follower() -- become follower first
	self:initial_set_timeout()
	while self.alive do
		clock.sleep(tick_intval_sec)
		self:tick()
	end
	self:fin()
end
function raft_index:start_election()
	self.election_thread = tentacle(self.launch_election, self)
end
function raft_index:launch_election()
	pcall(self.run_election, self)
	self.election_thread = nil
end
function raft_index:run_election()
	local myid = self.manager
	self:reset_timeout()
	logger.notice('raft', self:gid_for_log(), 'start election', myid)
	while self.state:is_candidate() do
		self.state:new_term()
		self.state:vote_for_self()
		local set = self.state.replica_set
		local myid_pos
		local votes = {}
		local quorum = math.ceil((#set + 1) / 2)
		for i=1,#set,1 do
			logger.debug('send req vote to', set[i])
			if not uuid.equals(set[i], myid) then
				table.insert(votes, set[i].async_rpc(
					REQUEST_VOTE,
					self.id,
					self.state:current_term(), 
					myid, self.state.wal:last_index_and_term()
				))
			else
				myid_pos = i
			end
		end
		local grant = 1 -- for vote of this node
		if #votes > 0 then
			local timeout = self.opts.election_timeout_sec
			local ret = event.join(clock.alarm(timeout), unpack(votes))
			if not self.state:is_candidate() then
				-- receive append_entries or request_vote during election
				-- it is also possible that this node starts election and get majority of the term here.
				-- but if it takes long time, another follower node may get timeout again (at here, heartbeat RPC have not started yet), 
				-- and start election with higher term.
				-- then this node may receive request vote from another node (with higher term), and become follower.
				logger.notice('raft', self:gid_for_log(), 'another leader seems to be elected (or on going)')
				break
			end
			local id
			for i=1,#ret-1 do -- -1 to ignore last result (is timeout event)
				local tp, obj, ok, term, granted, id = unpack(ret[i])
				for j=1,#votes do
					if votes[j] == obj then
						id = set[(j < myid_pos) and j or j + 1]
					end
				end
				logger.info('vote result', self:gid_for_log(), id, tp, ok, term, granted)
				-- not timeout and call itself success and vote granted
				if tp ~= 'timeout' and ok and granted then
					grant = grant + 1
				end
			end
		end
		-- check still candidate (to prevent error in become_leader())
		if grant >= quorum then
			logger.notice('raft', self:gid_for_log(), 'get quorum: become leader', grant, quorum)
			self.state:become_leader()
			break
		else
			logger.notice('raft', self:gid_for_log(), 'cannot get quorum: re-election', grant, quorum)
		end
		-- if election fails, give chance to another candidate.
		clock.sleep(util.random_duration(self.opts.election_timeout_sec))
	end
end
function raft_index:stop_replicator(target_actor)
	self.state:stop_replicator(target_actor)
end
function raft_index:read(timeout, ...)
	local l, timeout = self.state:request_routing_id(timeout or self.opts.proposal_timeout_sec)
	if l then return l:read(timeout, ...) end
	return self.state.fsm:fetch_state(...)
end
function raft_index:make_response(ok, r, ...)
	if not ok then
		error(r)
	else
		return r, ...
	end
end
-- async_propose asynchronously process proposed log. 
-- returns event which is emitted when operation complete.
function raft_index:async_propose(...)
	return tentacle(self.propose, self, ...)
end
function raft_index:propose(logs, timeout, dictatorial)
	if not self.alive then exception.raise('raft_invalid') end
	local l, timeout = self.state:request_routing_id(timeout or self.opts.proposal_timeout_sec)
	if l then return l.rpc(INTERNAL_PROPOSE, self.id, logs, timeout) end
	local msgid = router.regist(tentacle.running(), timeout + clock.get())
	-- ...then write log and kick snapshotter/replicator
	if dictatorial then
		self.state:dictatorial_write_logs(msgid, logs)
	else
		self.state:write_logs(msgid, logs)
	end
	-- wait until logs are committed
	return self:make_response(tentacle.yield(msgid))
end
raft_index.write = raft_index.propose
function raft_index:add_replica_set(replica_set, timeout)
	if not self.alive then exception.raise('raft_invalid') end
	local l, timeout = self.state:request_routing_id(timeout or self.opts.proposal_timeout_sec)
	if l then return l.rpc(INTERNAL_ADD_REPLICA_SET, self.id, replica_set, timeout) end
	local msgid = router.regist(tentacle.running(), timeout + clock.get())
	-- ...then write log and kick snapshotter/replicator
	self.state:add_replica_set(msgid, replica_set)
	-- wait until logs are committed
	return self:make_response(tentacle.yield(msgid))
end
function raft_index:remove_replica_set(replica_set, timeout)
	if not self.alive then exception.raise('raft_invalid') end
	if self.state:is_leader() then
		if type(replica_set) ~= 'table' then
			replica_set = {replica_set}
		end
		for i=1,#replica_set do
			if uuid.equals(replica_set[i], self.state:leader()) then
				logger.warn('leader try to remove itself, stepdown first', self.state:leader(), debug.traceback())
				self:stepdown()
				logger.warn('finish to stepdown now leader:', self.state:leader())
				break
			end
		end
	end
	local l, timeout = self.state:request_routing_id(timeout or self.opts.proposal_timeout_sec)
	if l then return l.rpc(INTERNAL_REMOVE_REPLICA_SET, self.id, replica_set, timeout) end
	local msgid = router.regist(tentacle.running(), timeout + clock.get())
	-- ...then write log and kick snapshotter/replicator
	self.state:remove_replica_set(msgid, replica_set)
	-- wait until logs are committed
	return self:make_response(tentacle.yield(msgid))
end
function raft_index:accepted()
	local a = self.state.proposals.accepted
	local ok, r
	for i=1,#a do
		local log = a[i]
		a[i] = nil
		-- proceed commit log index
		self.state:committed(log.index)
		-- apply log and respond to waiter
		for idx=tonumber(self.state:last_applied_index()) + 1, tonumber(log.index) do
			-- logger.info('apply_log', i, log.msgid)
			self:apply_log(self.state.wal:at(idx))
		end
	end
	self.state:kick_replicator()
end
function raft_index:apply_log(log)
	if log.msgid then
		router.respond_by_msgid(log.msgid, self.state:apply(log))
	else
		self.state:apply(log)
	end
end
-- access internal data (mainly for debugging purpose)
function raft_index:leader()
	return self.state:leader()
end
function raft_index:is_leader()
	return self.state:is_leader()
end
function raft_index:replica_set()
	return self.state.replica_set
end
function raft_index:probe(prober, ...)
	return pcall(prober, self, ...)
end
function raft_index:stepdown(timeout)
	if not self.alive then exception.raise('raft_invalid') end
	timeout = timeout or self.opts.proposal_timeout_sec
	local msgid = router.regist(tentacle.running(), timeout + clock.get())
	assert(self.state:is_leader(), "invalid node try to stepdown")
	self.state:become_follower(msgid)
	return self:make_response(tentacle.yield(msgid))
end
function raft_index:become_follower()
	self.state:become_follower()
end
--[[--
from https://ramcloud.stanford.edu/raft.pdf
--]]--
--[[
Append Entries RPC
1. Reply false if term < currentTerm (§5.1)
2. Reply false if log doesn’t contain an entry at prevLogIndex
whose term matches prevLogTerm (§5.3)
3. If an existing entry conflicts with a new one (same index
but different terms), delete the existing entry and all that
follow it (§5.3)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
]]
function raft_index:append_entries(term, leader, leader_commit_idx, prev_log_idx, prev_log_term, entries, added_idx)
	local ok, r
	local last_index, last_term = self.state.wal:last_index_and_term()
	if added_idx then
		self.state:set_added_index(added_idx)
	end
	if term < self.state:current_term() then
		-- 1. Reply false if term < currentTerm (§5.1)
		logger.warn('raft', self:gid_for_log(), 'append_entries', 'receive older term', term, self.state:current_term())
		return self.state:current_term(), false, last_index
	end
	-- (part of 2.) If AppendEntries RPC received from new leader: convert to follower
	if term > self.state:current_term() then
		self.state:become_follower()
		self.state:set_term(term)
	end
	-- Save the current leader
	self.state:set_leader(leader)
	-- if prev_log_idx is not set, means heartbeat. return.
	if prev_log_idx and prev_log_idx > 0 then
		-- verify last index and term. this node's log term at prev_log_idx should be same as which leader sent.
		local tmp_prev_log_term
		if prev_log_idx == last_index then
			-- skip access wal 
			tmp_prev_log_term = last_term
		else
			local log = self.state.wal:at(prev_log_idx)
			if not log then
				logger.warn('raft', self:gid_for_log(), 'fail to get prev log', prev_log_idx)
				return self.state:current_term(), false, last_index	
			end
			tmp_prev_log_term = log.term
		end
		if tmp_prev_log_term ~= prev_log_term then
			-- 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
			logger.warn('raft', self:gid_for_log(), 'last term does not match', tmp_prev_log_term, prev_log_term)
			return self.state:current_term(), false, last_index
		end
	end
	-- 3. If an existing entry conflicts with a new one (same index but different terms), 
	-- delete the existing entry and all that follow it (§5.3)
	if entries and #entries > 0 then
		local first, last = entries[1], entries[#entries]
		-- Delete any conflicting entries
		if first.index <= last_index then
			logger.warn('raft', self:gid_for_log(), 'clearing log suffix range', first.index, last_index)
			local wal = self.state.wal
			ok, r = pcall(wal.delete_range, wal, first.index, last_index)
			if not ok then
				logger.error('raft', self:gid_for_log(), 'failed to clear log suffix', r)
				return self.state:current_term(), false, last_index
			end
		end

		-- 4. Append any new entries not already in the log
		if not self.state.wal:copy(entries) then
			logger.error('raft', self:gid_for_log(), 'failed to append logs')
			return self.state:current_term(), false, last_index
		end
	end

	-- 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	-- logger.info('commits', leader_commit_idx, self.state:last_commit_index(), self.state:last_applied_index(), self.state:last_index())
	if leader_commit_idx and (leader_commit_idx > self.state:last_commit_index()) then
		local new_last_commit_idx = math.min(tonumber(leader_commit_idx), tonumber(self.state:last_index()))
		self.state:set_last_commit_index(new_last_commit_idx) -- no error check. force set leader value.
		for idx=tonumber(self.state:last_applied_index()) + 1, new_last_commit_idx do
			-- logger.info('apply_log', idx)
			self:apply_log(self.state.wal:at(idx))
		end
	end
	-- reset timeout to prevent election timeout
	self:reset_timeout()
	-- Everything went well, return success
	return self.state:current_term(), true, self.state:last_index()
end
--[[
Request Vote RPC
1. Reply false if term < currentTerm (§5.1)
2. If votedFor is null or candidateId, and candidate’s log is at
least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
]]
function raft_index:request_vote(term, candidate_id, cand_last_log_idx, cand_last_log_term)
	logger.debug('request_vote from', candidate_id, term)
	local last_index, last_term = self.state.wal:last_index_and_term()
	if term < self.state:current_term() then
		-- 1. Reply false if term < currentTerm (§5.1)
		logger.warn('raft', self:gid_for_log(), 'request_vote', 'receive older term', term, self.state:current_term())
		return self.state:current_term(), false
	end
	if term > self.state:current_term() then
		self.state:become_follower()
		self.state:set_term(term)
	end
	-- and candidate’s log is at least as up-to-date as receiver’s log, 
	if cand_last_log_idx < last_index then
		logger.warn('raft', self:gid_for_log(), 'request_vote', 'log is not up-to-date', cand_last_log_idx, last_index)
		return self.state:current_term(), false		
	end
	if cand_last_log_term < last_term then
		logger.warn('raft', self:gid_for_log(), 'request_vote', 'term is not up-to-date', cand_last_log_term, last_term)
		return self.state:current_term(), false		
	end
	-- 2. If votedFor is null or candidateId, 
	if not self.state:vote_for(candidate_id, term) then
		logger.warn('raft', self:gid_for_log(), 'request_vote', 'already vote', v, candidate_id, term)
		return self.state:current_term(), false
	end
	-- grant vote (§5.2, §5.4)
	logger.debug('raft', self:gid_for_log(), 'request_vote', 'vote for', candidate_id, term)
	return self.state:current_term(), true
end
function raft_index:install_snapshot(term, leader, last_snapshot_index, fd)
	-- Ignore an older term
	if term < self.state:current_term() then
		logger.warn('raft', 'install_snapshot', 'receive older term', term, self.state:current_term())
		return
	end
	-- Increase the term if we see a newer one
	if term > self.state:current_term() then
		self.state:become_follower()
		self.state:set_term(term)
	end
	-- Save the current leader
	self.state:set_leader(leader)
	-- Spill the remote snapshot to disk
	local ok, rb = pcall(self.snapshot.copy, self.snapshot, fd, last_snapshot_index) 
	if not ok then
		self.snapshot:remove_tmp()
		logger.error("raft", self:gid_for_log(), 'install_snapshot', "Failed to copy snapshot", rb)
		return
	end
	-- Restore snapshot
	self.state:restore_from_snapshot(rb)
	-- Update the lastApplied so we don't replay old logs
	self.state.last_applied_idx = last_snapshot_index
	-- Compact logs, continue even if this fails
	self.state.wal:compaction(last_snapshot_index)
	-- reset timeout to prevent election timeout
	self:reset_timeout()
	logger.debug("raft", self:gid_for_log(), 'install_snapshot', "Installed remote snapshot")
	return true
end

-- module functions
local default_opts = {
	logsize_snapshot_threshold = 10000,
	initial_proposal_size = 1024,
	log_compaction_margin = 10240, 
	snapshot_file_preserve_num = 3, 
	election_timeout_sec = 1.0,
	heartbeat_timeout_sec = 0.1,
	proposal_timeout_sec = 5.0,
	serde = "msgpack",
	storage = "rocksdb", 
	datadir = luact.DEFAULT_ROOT_DIR,
	debug_dictatorial = false,
}
local function configure_datadir(id, opts)
	local strid = tostring(ffi.cast('luact_uuid_t *', id))
	if not opts.datadir then
		exception.raise('invalid', strid, 'config', 'options must contain "datadir"')
	end
	local p = fs.path(opts.datadir, tostring(pulpo.thread_id), "raft")
	logger.notice('raft datadir', p, 'for group', strid)
	return p
end
local function configure_serde(opts)
	return serde[serde.kind[opts.serde]]
end
local raft_manager
_M.default_opts = default_opts
function _M.new(id, fsm_factory, opts, ...)
	if not raft_bodymap[id] then
		local ma = _M.manager_actor()
		opts = util.merge_table(_M.default_opts, opts or {})
		local fsm = (type(fsm_factory) == 'function' and fsm_factory(...) or fsm_factory)
		local dir = configure_datadir(id, opts)
		local sr = configure_serde(opts)
		-- NOTE : this operation may *block* 100~1000 msec (eg. rocksdb store initialization) in some environment
		-- create column_family which name is "id" in database at dir
		-- TODO : use unified column family for raft stable storage. related data such as hardstate/log key will be prefixed by "id".
		local store = (require ('luact.cluster.store.'..opts.storage)).new(dir, tostring(id))
		local ss = snapshot.new(dir, sr)
		local wal = wal.new(fsm:metadata(), store, sr, opts)
		local rft = setmetatable({
			id = id,
			manager = ma,
			state = state.new(id, fsm, wal, ss, opts), 
			-- TODO : finalize store when this raft group no more assigned to this node.
			store = store,
			opts = opts,
			alive = true,
			timeout_limit = 0,
		}, raft_mt)
		rft.state.controller = rft
		rft:start()
		raft_bodymap[id] = rft
	end
	return raft_manager
end
function _M.manager_actor()
	if not raft_manager then
		raft_manager = luact.supervise(_M, { 
			actor = {
				uuid = actor.system_process_of(
					nil, luact.thread_id, luact.SYSTEM_PROCESS_RAFT_MANAGER
				)
			},
		})
	end
	return raft_manager
end
local function find_body(id)
	return raft_bodymap[id]
end
-- id : uuid
function _M.destroy(id)
	local rft = find_body(id)
	if rft then
		rft:destroy()
	end
end
-- get raft body. internal use only. (id : uuid)
_M._find_body = find_body

function _M.rpc(kind, id, ...)
	local rft = raft_bodymap[id]
	if not rft then
		exception.raise('raft_not_found', id, kind)
	end
	-- rpc for consensus
	if kind == APPEND_ENTRIES then
		return rft:append_entries(...)
	elseif kind == REQUEST_VOTE then
		return rft:request_vote(...)
	elseif kind == INSTALL_SNAPSHOT then
		return rft:install_snapshot(...)
	-- internal rpcs
	elseif kind == INTERNAL_ACCEPTED then
		return rft:accepted()
	elseif kind == INTERNAL_PROPOSE then
		return rft:propose(...)
	elseif kind == INTERNAL_ADD_REPLICA_SET then
		return rft:add_replica_set(...)
	elseif kind == INTERNAL_REMOVE_REPLICA_SET then
		return rft:remove_replica_set(...)
	else
		exception.raise('raft_invalid_rpc', kind)
	end
end

return _M
