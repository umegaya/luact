local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local fs = require 'pulpo.fs'

local ringbuf = require 'luact.util.ringbuf'
local rpc = require 'luact.cluster.raft.rpc'
local pbuf = require 'luact.pbuf'

local _M = {}


-- cdef
ffi.cdef [[
typedef struct luact_raft_proposal_state {
	uint16_t quorum, current;
} luact_raft_proposal_state_t;
typedef struct luact_raft_proposal_list {
	luact_raft_proposal_state_t states[0];
} luact_raft_proposal_list_t;
]]


-- luact_raft_proposal_state_t
local proposal_state_index = {}
local proposal_state_mt = {
	__index = proposal_state_index
}
function proposal_state_index:init(q)
	self.quorum = q
	self.current = 0
end
function proposal_state_index:granted()
	return self:valid() and self.quorum <= self.current
end
function proposal_state_index:commit()
	self.current = self.current + 1
end
function proposal_state_index:valid() 
	return self.quorum > 0 
end
ffi.metatype('luact_raft_proposal_state_t', proposal_state_mt)


-- luact_raft_proposals_t
local proposal_list_index = {}
local proposal_list_mt = {
	__index = proposal_list_index
}
local function proposal_list_size(size)
	return ffi.sizeof('luact_raft_proposal_list_t') + (size * ffi.sizeof('luact_raft_proposal_state_t'))
end
local function proposal_list_alloc(size)
	return ffi.cast('luact_raft_proposal_list_t*', memory.alloc(proposal_list_size(size)))
end
function proposal_list_index:copy(src_store, src_pos, dst_pos)
	self.states[dst_pos] = src_store.states[src_pos]
end
function proposal_list_index:get_by_pos(i)
	return self.states[i]
end
function proposal_list_index:set_by_pos(i, v)
	self.states[i] = v
end
function proposal_list_index:delete(i)
	self.states[i].quorum = 0
end
proposal_list_index.alloc = proposal_list_alloc
function proposal_list_index:fin()
	memory.free(self)
end
function proposal_list_index:from(spos, epos)
	exception.raise('fatal', 'from is not implemented')
end
ffi.metatype('luact_raft_proposal_list_t', proposal_list_mt)


-- proposals object
local proposals_index = {}
local proposals_mt = {
	__index = proposals_index
}
function proposals_index:fin()
	memory.free(self.progress.store)
end
-- have to call at least leader commits logs of start_idx ~ end_idx
function proposals_index:add(quorum, start_idx, end_idx)
	for idx = tonumber(start_idx), tonumber(end_idx) do
		self.progress:init_at(idx, function (st, q)
			st:init(q)
			st:commit() -- leader node already commit these entry.
		end, quorum)
	end
end
function proposals_index:dictatorial_add(controller, start_idx, end_idx)
	for idx = tonumber(start_idx), tonumber(end_idx) do
		self.progress:init_at(idx, function (st, q)
			st:init(1)
		end, quorum)
	end
	self:range_commit(controller, start_idx, end_idx, true)
end
function proposals_index:commit(index)
	local header = self.progress.header
	local prev_start_idx = header.start_idx
	local st, last_commit_idx

	st = self.progress:at(index)
	if not st then
		-- logger.warn('commit: no status at:', index)
		goto notice
	end
	st:commit()
	-- logger.warn('commit result', index, st.quorum, st.current, st:granted())
	if not st:granted() then
		goto notice
	end
	-- commit order changed: eg) # of quorum changes
	-- wait for all previous proposals are committed
	if index ~= header.start_idx then
		goto notice
	end
	while true do
		last_commit_idx = header.start_idx
		self.progress:delete_range(nil, header.start_idx)
		-- header.start_idx must be increment (+1)
		if header.start_idx >= header.end_idx then
			break -- finish processing all proposal
		end
		st = self.progress:at(header.start_idx)
		if (not st) or (not st:granted()) then
			goto notice
		end
	end

::notice::
	-- logger.info('last_commit_idx', tostring(prev_start_idx), tostring(last_commit_idx))
	if last_commit_idx then
		for idx=tonumber(prev_start_idx),tonumber(last_commit_idx) do
			-- logger.info('idx', idx, 'accepted')
			local log = self.wal:at(idx)
			table.insert(self.accepted, log)
		end
		return true
	end
end
function proposals_index:range_commit(controller, sidx, eidx, delay)
	local accepted
	for i=tonumber(sidx),tonumber(eidx) do
		if self:commit(i) then
			accepted = true
		end
	end
	if accepted then
		logger.debug('notify_accepted', controller)
		if delay then
			controller.manager.notify_rpc(rpc.INTERNAL_ACCEPTED, controller.id)
		else
			controller:accepted()
		end
	end
end

function _M.new(wal, size)
	return setmetatable({
		progress = ringbuf.new(size, proposal_list_alloc(size)), 
		wal = wal,
		accepted = {},
	}, proposals_mt)
end

return _M
