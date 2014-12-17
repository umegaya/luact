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

local _M = {}
local raftmap = {}

-- tick interval
local tick_intval_sec = 0.05

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
function raft_index:__actor_destroy__()
	self.alive = nil
end
function raft_index:check_election_timeout()
	-- between x msec ~ 2*x msec
	return (self.elapsed >= util.random(self.election_timeout, self.election_timeout * 2 - 1))
end
function raft_index:tick()
	self.elapsed = self.elapsed + 1
	if self.state:is_follower() then
		-- logger.info('follower no heartbeat elapsed', self.elapsed)
		if self:check_election_timeout() then
			logger.info('follower election timeout', self.elapsed)
			-- if election timeout, become candidate
			self.state:become_candidate()
		end
	end
end
function raft_index:start()
	tentacle(self.run, self)
end
function raft_index:run()
	self.state:become_follower() -- become follower first
	while self.alive do
		clock.sleep(tick_intval_sec)
		self:tick()
	end
	self:fin()
end
function raft_index:start_election()
	while self.state:is_candidate() do
		self.state:new_term()
		self.state:vote_for(actor.of(self))
		self.elapsed = 0
		local set = self.state.replica_set
		local votes = {}
		local quorum = math.ceil((#set + 1) / 2)
		for i=1,#set,1 do
			table.insert(votes, set[i]:async_request_vote())
		end
		local timeout = self.election_timeout_sec
		local ret = event.join(clock.alarm(timeout), unpack(votes))
		local grant = 1 -- for vote of this node
		local r = ret[#ret] -- last event is timeout event
		for i=1,#ret-1 do -- -1 to ignore last result (is timeout event)
			local r = ret[i]
			if r[1] ~= 'timeout' then
				grant = grant + 1
			end
		end
		if grant >= quorum then
			logger.notice('get quorum: become leader:', grant, quorum)
			self.state:become_leader()
			break
		end
	end
end
function raft_index:propose(logs, timeout)
	local l = self.state:request_routing_id()
	if l then return l:propose(logs, timeout) end
	local msgid = router.regist(coroutine.running(), timeout or self.proposal_timeout)
	-- ...then write log and kick snapshotter/replicator
	self.state:write_logs(msgid, logs)
	-- wait until logs are committed
	return coroutine.yield()
end
function raft_index:add_replica_set(replica_set, timeout)
	-- routing request to leader
	local l = self.state:request_routing_id()
	if l then return l:add_replica_set(replica_set, timeout) end
	local msgid = router.regist(coroutine.running(), timeout or self.proposal_timeout)
	-- ...then write log and kick snapshotter/replicator
	self.state:add_replica_set(msgid, replica_set)
	-- wait until logs are committed
	return coroutine.yield()
end
function raft_index:remove_replica_set(replica_set, timeout)
	-- routing request to leader
	local l = self.state:request_routing_id()
	if l then return l:remove_replica_set(replica_set, timeout) end
	local msgid = router.regist(coroutine.running(), timeout or self.proposal_timeout)
	-- ...then write log and kick snapshotter/replicator
	self.state:remove_replica_set(msgid, replica_set)
	-- wait until logs are committed
	return coroutine.yield()
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
		self:apply_log(log)
	end
end
function raft_index:apply_log(log)
	-- apply log to fsm
	local ok, r = self.state:apply(log)
	if log.msgid then
		router.respond_by_msgid(log.msgid, ok, r)
	end
end
-- access internal data (mainly for debugging purpose)
function raft_index:leader()
	return self.state:leader()
end
function raft_index:replica_set()
	return self.state.replica_set
end
function raft_index:probe_fsm(prober)
	return prober(self.state.fsm)
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
function raft_index:append_entries(term, leader, leader_commit_idx, prev_log_idx, prev_log_term, entries)
	local ok, r
	local last_index, last_term = self.state.wal:last_index_and_term()
	if term < self.state:current_term() then
		-- 1. Reply false if term < currentTerm (§5.1)
		logger.warn('raft', 'append_entries', 'receive older term', term, self.state:current_term())
		return self.state:current_term(), false, last_index
	end
	-- (part of 2.) If AppendEntries RPC received from new leader: convert to follower
	if term > self.state:current_term() then
		self.state:become_follower()
		self.state:set_term(term)
	end
	-- Save the current leader
	self.state:set_leader(leader)
	-- reset elapsed time to prevent election timeout
	self.elapsed = 0
	-- if prev_log_idx is not set, means heartbeat. return.
	if prev_log_idx then
		-- verify last index and term. this node's log term at prev_log_idx should be same as which leader sent.
		local tmp_prev_log_term
		if prev_log_idx == last_index then
			-- skip access wal 
			tmp_prev_log_term = last_term
		else
			local log = self.state.wal:at(prev_log_idx)
			if not log then
				logger.warn('raft', 'fail to get prev log', prev_log_idx)
				if pulpo.thread_id == 1 then
					self.state.wal:dump()
				end
				return self.state:current_term(), false, last_index	
			end
			tmp_prev_log_term = log.term
		end
		if tmp_prev_log_term ~= prev_log_term then
			-- 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
			logger.warn('raft', 'last term does not match', tmp_prev_log_term, prev_log_term)
			return self.state:current_term(), false, last_index
		end
	end
	-- 3. If an existing entry conflicts with a new one (same index but different terms), 
	-- delete the existing entry and all that follow it (§5.3)
	if entries and #entries > 0 then
		local first, last = entries[1], entries[#entries]
		-- Delete any conflicting entries
		if first.index <= last_index then
			logger.warn('raft', 'Clearing log suffix range', first.index, last_index)
			ok, r = self.state.wal:delete_logs(first.index, last_index)
			if not ok then
				logger.error('raft', 'Failed to clear log suffix', r)
				return self.state:current_term(), false, last_index
			end
		end

		-- 4. Append any new entries not already in the log
		if not self.state.wal:copy(entries) then
			logger.error('raft', 'Failed to append to logs')
			return self.state:current_term(), false, last_index
		end
	end

	-- 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	-- logger.info('commits', leader_commit_idx, self.state:last_commit_index(), self.state:last_applied_index(), self.state:last_index())
	if leader_commit_idx and (leader_commit_idx > self.state:last_commit_index()) then
		local new_last_commit_idx = math.min(tonumber(leader_commit_idx), tonumber(self.state:last_index()))
		self.state:set_last_commit_index(new_last_commit_idx) -- no error check. force set leader value.
		for idx=tonumber(self.state:last_applied_index()) + 1, new_last_commit_idx do
			-- logger.info('apply_log', i)
			self:apply_log(self.state.wal:at(idx))
		end
	end

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
	local last_index, last_term = self.state.wal:last_index_and_term()
	if term < self.state:current_term() then
		-- 1. Reply false if term < currentTerm (§5.1)
		logger.warn('raft', 'request_vote', 'receive older term', term, self.state:current_term())
		return self.state:current_term(), false
	end
	if term > self.state:current_term() then
		self.state:become_follower()
		self.state:set_term(term)
	end
	-- and candidate’s log is at least as up-to-date as receiver’s log, 
	if cand_last_log_idx >= last_index then
		logger.warn('raft', 'log is not up-to-date', cand_last_log_idx, last_index)
		return self.state:current_term(), false		
	end
	local log = self.state.wal:at(last_log_idx)
	if cand_last_log_term ~= log.term then
		logger.warn('raft', 'same index but term not matched', cand_last_log_term, log.term)
		return self.state:current_term(), false		
	end
	-- 2. If votedFor is null or candidateId, 
	if not self.state:vote_for(candidate_id, term) then
		logger.warn('raft', 'already vote', v, candidate_id, term)
		return self.state:current_term(), false
	end
	-- grant vote (§5.2, §5.4)
	logger.notice('raft', 'vote for', candidate_id)
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
		logger.error("raft", "Failed to copy snapshot", rb)
		return
	end
	-- Restore snapshot
	self.state:restore_from_snapshot(rb)
	-- Update the lastApplied so we don't replay old logs
	self.state.last_applied_idx = last_snapshot_index
	-- Compact logs, continue even if this fails
	self.state.wal:compaction(last_snapshot_index)
	
	logger.info("raft", "Installed remote snapshot")
	return true
end

-- module functions
local default_opts = {
	logsize_snapshot_threshold = 10000,
	initial_proposal_size = 1024,
	log_compaction_margin = 10240, 
	snapshot_file_preserve_num = 3, 
	election_timeout_sec = 1.0,
	heartbeat_timeout_sec = 1.0,
	proposal_timeout_sec = 5.0,
	serde = "serpent",
	storage = "rocksdb",
	work_dir = luact.DEFAULT_ROOT_DIR,
}
local function configure_workdir(id, opts)
	if not opts.work_dir then
		exception.raise('invalid', 'config', 'must contain "workdir"')
	end
	local p = fs.path(opts.work_dir, tostring(pulpo.thread_id), tostring(id))
	logger.notice('raft workdir', id, p)
	return p
end
local function configure_serde(opts)
	return serde[serde.kind[opts.serde]]
end
local function configure_timeout(opts)
	return math.floor(opts.election_timeout_sec / tick_intval_sec), 
		math.floor(opts.heartbeat_timeout_sec / tick_intval_sec)
end
local function create(id, fsm_factory, opts, ...)
	local fsm = fsm_factory(...)
	local dir = configure_workdir(id, opts)
	local sr = configure_serde(opts)
	local election, heartbeat = configure_timeout(opts)
	local store = (require ('luact.cluster.store.'..opts.storage)).new(dir, tostring(pulpo.thread_id))
	local ss = snapshot.new(dir, sr)
	local wal = wal.new(fsm:metadata(), store, sr, opts)
	local rft = setmetatable({
		state = state.new(fsm, wal, ss, opts), 
		proposal_timeout = opts.proposal_timeout_sec,
		election_timeout_sec = opts.election_timeout_sec,
		election_timeout = election,
		heartbeat_timeout = heartbeat,
		alive = true,
		elapsed = 0, 
	}, raft_mt)
	rft.state.actor_body = rft
	return rft
end
-- create new raft state machine
_M.default_opts = default_opts
_M.create_ev = event.new()
function _M.new(id, fsm_factory, opts, ...)
	local rft = raftmap[id]
	opts = opts or default_opts
	if not rft then
		if rft == nil then
			raftmap[id] = false
			rft = luact.supervise(create, opts.supervise_options, id, fsm_factory, opts, ...)
			raftmap[id] = rft
			rft:start()
			_M.create_ev:emit('create', id, rft)
		else
			local create_id
			while true do
				create_id, rft = select(3, event.join(clock.alarm(5.0), _M.create_ev))
				if not create_id then
					exception.raise('raft', 'object creation timeout')
				end
				if id == create_id then
					break
				end
			end
		end
	end
	return rft
end
function _M.find(id)
	return raftmap[id]
end
function _M.destroy(id)
	local rft = raftmap[id]
	if rft then
		raftmap[id] = nil
		actor.destroy(rft)
	end
end

return _M
