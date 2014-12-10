local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local fs = require 'pulpo.fs'

local ringbuf = require 'luact.util.ringbuf'
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
local function proposal_list_realloc(p, size)
	return ffi.cast('luact_raft_proposal_list_t*', memory.realloc(p, proposal_list_size(size)))
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
proposal_list_index.realloc = proposal_list_realloc
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
function proposals_index:add(quorum, start_idx, end_idx)
	for idx = tonumber(start_idx), tonumber(end_idx) do
		self.progress:init_at(idx, function (st, q)
			st:init(q)
		end, quorum)
	end
end
function proposals_index:commit(index)
	local header = self.progress.header
	local prev_start_idx = header.start_idx
	local st, last_commit_idx

	st = self.progress:at(index)
	if not st then
		goto notice
	end
	st:commit()
	-- print('commit result', index, st.quorum, st.current, st:granted())
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
		self.progress:delete_elements(header.start_idx)
		-- header.start_idx must be increment (+1)
		st = self.progress:at(header.start_idx)
		if (not st) or (not st:granted()) then
			goto notice
		end
	end

::notice::	
	if last_commit_idx then
		for idx=tonumber(prev_start_idx),tonumber(last_commit_idx) do
			local log = self.wal:at(idx)
			table.insert(self.accepted, log)
		end
		return true
	end
end
function proposals_index:range_commit(actor, sidx, eidx)
	local accepted
	for i=tonumber(sidx),tonumber(eidx) do
		if self:commit(i) then
			accepted = true
		end
	end
	if accepted then
		actor:notify_accepted()
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
