local luact = require 'luact.init'
local uuid = require 'luact.uuid'
local clock = require 'luact.clock'
local serde = require 'luact.serde'
local router = require 'luact.router'

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
	raft_index = raft_index
}
function raft_index:init()
end
function raft_index:fin()
	state:fin()
end
function raft_index:__actor_destroy__()
	self.alive = nil
end
function raft_index:election_timeout()
	-- between x msec ~ 2*x msec
	return (self.elapsed >= util.random(_M.election_timeout, _M.election_timeout * 2 - 1))
end
function raft_index:tick()
	self.elapsed = self.elapsed + 1
	if self:is_follower() then
		if self:election_timeout() then
			-- if election timeout, become candidate
			self.state:become_candidate()
		end
	end
end
function raft_index:run()
	self:become_follower() -- become follower first
	while self.alive do
		clock.sleep(tick_intval_sec)
		self:tick()
	end
	self:fin()
end
function raft_index:start_election()
	while self.state:is_candidate() do
		self.elapsed = 0
		local set = self.state.replica_set
		local votes = {}
		local quorum = math.ceil((#set + 1) / 2)
		for i=1,#set,1 do
			table.insert(votes, set[i]:async_request_vote())
		end
		local timeout = math.random(self.election_timeout_sec, self.election_timeout_sec * 2)
		local ret = event.wait(clock.alarm(timeout), unpack(votes))
		local grant = 0
		for i=1,#ret-1 do
			local r = ret[i]
			if r[1] ~= 'timeout' then
				grant = grant + 1
			end
		end
		if grant >= quorum then
			self.state:become_leader()
			break
		end
	end
end
function raft_index:propose(logs, timeout)
	local l = self.state:leader()
	-- routing request to leader
	if l then return l:propose(logs, timeout) end
	local msgid = router.regist(coroutine.running(), timeout or self.proposal_timeout)
	-- ...then write log and kick snapshotter/replicator
	self.state:write_logs(msgid, logs)
	-- wait until logs are committed
	return coroutine.yield()
end
function raft_index:add_replica_set(replica_set, timeout)
	-- routing request to leader
	if not self.state:is_leader() then 
		return self.state:leader():add_replica_set(replica_set, timeout) 
	end
	local msgid = router.regist(coroutine.running(), timeout or self.proposal_timeout)
	-- ...then write log and kick snapshotter/replicator
	self.state:add_replica_set(msgid, replica_set)
	-- wait until logs are committed
	return coroutine.yield()
end
function raft_index:remove_replica_set(replica_set, timeout)
	-- routing request to leader
	if not self.state:is_leader() then 
		return self.state:leader():remove_replica_set(replica_set, timeout) 
	end
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
		self:apply_log(log)
	end
end
function raft_index:apply_log(log)
	-- proceed commit log index
	self.state:committed(log.index)
	if not log.kind then
		-- apply to fsm
		ok, r = pcall(self.state.apply, self.state, log)
	else
		-- apply to raft itself
		ok, r = pcall(self.state.apply_system, self.state, log)
	end
	if log.msgid then
		router.respond_by_msgid(log.msgid, ok, r)
	end
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
function raft_index:append_entries(term, leader, prev_log_idx, prev_log_term, entries, leader_commit_idx)
	local ok, r
	local last_index, last_term = self.state.wal:last_index_and_term()
	if term < self.state:current_term() then
		-- 1. Reply false if term < currentTerm (§5.1)
		logger.warn('raft', 'receive older term', term, self.state:current_term())
		return self.state:current_term(), false, last_index
	end
	if term > self.state:current_term() then
		self.state:become_follower()
		self.state:set_term(term)
	end
	if prev_log_idx ~= last_index then
		local log = self.state.wal:at(prev_log_idx)
		last_term = log.term
	end
	if last_term ~= prev_log_term then
		-- 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		logger.warn('raft', 'last term does not match', last_term, prev_log_term)
		return self.state:current_term(), false, last_index
	end
	-- If AppendEntries RPC received from new leader: convert to follower
	if self.state:is_candidate() then
		self.state:become_follower()
	end
	-- 3. If an existing entry conflicts with a new one (same index but different terms), 
	-- delete the existing entry and all that follow it (§5.3)
	if #entries > 0 then
		local first, last = entries[1], entries[#entries]
		-- Delete any conflicting entries
		if first.index <= last_index {
			logger.warn('raft', 'Clearing log suffix range', first.index, last_index)
			ok, r = self.state.wal:delete_logs(first.index, last_index)
			if not ok then
				logger.error('raft', 'Failed to clear log suffix', r)
				return self.state:current_term(), false, last_index
			end
		end

		-- 4. Append any new entries not already in the log
		ok, r = pcall(self.state.write_logs, self.state, nil, entries)
		if not ok then
			logger.error('raft', 'Failed to append to logs', r)
			return self.state:current_term(), false, last_index
		end
	end

	-- 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if leader_commit_idx > 0 && leader_commit_idx > self.state:last_commit_index() then
		local idx = math.min(leader_commit_idx, self.state:last_index())
		for i=self.state:last_commit_index(), idx do
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
		logger.warn('raft', 'receive older term', term, self.state:current_term())
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
	if not self.state:vote(candidate_id, term) then
		logger.warn('raft', 'already vote', v, candidate_id, term)
		return self.state:current_term(), false
	end
	-- grant vote (§5.2, §5.4)
	logger.notice('raft', 'vote for', candidate_id)
	return self.state:current_term(), true
end
function raft_index:install_snapshot(term, leader, last_log_idx, last_log_term, nodes, size)
	self:verify_term(term)
	-- TODO : how to receive snapshot file itself? (using sliced bytes array?)
end

-- module functions
local default_opts = {
	logsize_snapshot_threshold = 10000,
	initial_proposal_size = 1024,
	log_compaction_margin = 10240, 
	snapshot_file_preserve_num = 3, 
	election_timeout_sec = 0.15,
	heartbeat_timeout_sec = 1.0,
	proposal_timeout_sec = 5.0,
	serde = "serpent",
	storage = "rocksdb",
	workdir = luact.DEFAULT_ROOT_DIR,
}
local function configure_workdir(id, opts)
	return fs.path(opts.work_dir, tostring(pulpo.thread_id), tostring(id))
end
local function configure_serde(opts)
	return serde[serde.kind[opts.serde]]
end
local function configure_timeout(opts)
	return math.floor(opts.election_timeout_sec / tick_intval_sec), 
		math.floor(opts.heartbeat_timeout_sec / tick_intval_sec)
end
local function create(id, fsm, opts)
	opts = opts or default_opts
	local dir = configure_workdir(id, opts)
	local sr = configure_serde(opts)
	local election, heartbeat = configure_timeout(opts)
	local store = (require ('luact.cluster.storage.'..opts.storage).new(dir, tostring(pulpo.thread_id))
	local ss = snapshot.new(dir, sr)
	local wal = wal.new(fsm:metadata(), store, sr, opts)
	local rft = setmetatable({
		state = state.new(fsm, wal, ss, opts), 
		proposal_timeout = opts.proposal_timeout_sec
		election_timeout = election,
		heartbeat_timeout = heartbeat,
	}, raft_mt)
	rft.state.actor_body = rft
	rft.state:restore()
	rft:run()
	return rft
end
-- create new raft state machine
function _M.new(id, fsm, opts)
	local rft = raftmap[id]
	if not rft then
		rft = luact.supervise(create, id, fsm, opts)
		raftmap[id] = rft
	end
	return rft
end
function _M.find(id)
	return assert(raftmap[id], exception.new('not_found', 'raft group', id))
end
function _M.destroy(id)
	local rft = raftmap[id]
	if rft then
		actor.destroy(rft)
	end
end

return _M
