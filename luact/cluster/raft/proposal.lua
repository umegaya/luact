local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local pbuf = require 'pulpo.pbuf'
local fs = require 'pulpo.fs'

local ringbuf = require 'luact.cluster.raft.ringbuf'

local _M = {}


-- cdef
ffi.cdef [[
typedef enum luact_raft_proposal_state {
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
	return self.quorum <= self.current
end
function proposal_state_index:commit()
	self.current = self.current + 1
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
	local p = ffi.cast('luact_raft_proposal_list_t*', memory.alloc(proposal_list_size(size)))
	p:init(size)
	return p
end
local function proposal_list_realloc(p, size)
	return ffi.cast('luact_raft_proposal_list_t*', memory.realloc(p, proposal_list_size(size)))
end
function ringbuf_store_index:copy(src_store, src_pos, dst_pos)
	self.states[dst_pos] = src_store.states[src_pos]
end
function proposal_list_index:realloc(size)
	return proposal_list_realloc(self, size)
end
function proposal_list_index:fin()
	memory.free(self)
end
ffi.metatype('luact_raft_proposal_list_t', proposal_list_mt)


-- proposals object
local proposals_index = {}
local proposals_mt = {
	__index = proposal_list_index
}
function proposals_index:fin()
	memory.free(self.progress.store)
end
function proposals_index:add(quorum, logs)
	for _, log in ipairs(logs) do
		self.progress:init_at(log.index, function (st, q)
			st:init(q)
		end, quorum)
	end
end
function proposals_index:commit(index)
	local header,states = self.progress.header, self.progress.store
	local prev_start_idx = header.start_idx
	local pos, st, last_commit_idx

::again::
	if (header.start_idx > index) or (header.end_idx < index) then
		return last_commit_idx
	end
	pos = header:index2pos(index)
	st = states[pos]
	st:commit()
	if not st:granted() then
		goto notice
	end
	-- commit order changed: eg) # of quorum changes
	-- wait for all previous proposals are committed
	if index ~= header.start_idx then
		goto notice
	end
	last_commit_idx = header.start_idx
	header.start_idx = header.start_idx + 1
	index = index + 1
	goto again

::notice::	
	if last_commit_idx then
		for idx=prev_start_idx,last_commit_idx do
			local log = self.wal:at(idx)
			table.insert(self.accepted, log)
		end
		return true
	end
end
function proposals_index:range_commit(actor, sidx, eidx)
	local accepted
	for i=sidx,eidx,1 do
		if self:commit(i) then
			accepted = true
		end
	end
	if accepted then
		actor:notify_accepted()
	end
end

function _M.new(size)
	return setmetatable({
		progress = ringbuf.new(size, proposal_list_alloc(size))
		wal = wal,
		-- logs = {}, 
		accepted = {},
	}, proposals_mt)
end

return _M
