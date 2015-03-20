local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_luact(1, nil, function ()

local luact = require 'luact.init'
local range = require 'luact.cluster.dht.range'
local txncoord = require 'luact.storage.txncoord'
local tools = require 'test.tools.cluster'
local mvcc = require 'luact.storage.mvcc'
local event = require 'pulpo.event'
local fs = require 'pulpo.fs'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'
local lamport = require 'pulpo.lamport'
local test = tools.test_runner

-- register merger
mvcc.register_merger("inc", function (key, key_length, 
					existing, existing_length, 
					payload, payload_length,
					new_value_length)
	local add = tonumber(ffi.string(payload, payload_length))
	if not existing_length then
		add = tostring(add)
		new_value_length[0] = #add
		return true, add
	else
		local v = tonumber(ffi.string(existing, existing_length))
		local ret = tostring(v + add)
		new_value_length[0] = #ret
		return true, ret
	end
end)


-- use dummy arbiter
local g_last_ts 
tools.use_dummy_arbiter(nil, function (actor, log, timeout, dectatorial)
	g_last_ts = log[1].timestamp
end)

-- init range manager
fs.rmdir("/tmp/luact/range_test")
local range_manager = range.get_manager(nil, "/tmp/luact/range_test", { 
	n_replica = 1, -- allow single node quorum
	storage = "rocksdb",
	datadir = luact.DEFAULT_ROOT_DIR,
	range_size_max = 64 * 1024 * 1024
})

-- init txn coordinator
txncoord.initialize(range_manager)

-- setCorrectnessRetryOptions sets client for aggressive retries with a
-- limit on number of attempts so we don't get stuck behind indefinite
-- backoff/retry loops. If MaxAttempts is reached, transaction will
-- return retry error.
local retry_opts = {
	wait = 0.001,
	max_wait = 0.01,
	wait_multiplier = 2,
	max_attempt = 3,
}

-- The following structs and methods provide a mechanism for verifying
-- the correctness of Cockroach's transaction model. They do this by
-- allowing transaction histories to be specified for concurrent txns
-- and then expanding those histories to enumerate all possible
-- priorities, isolation levels and interleavings of commands in the
-- histories.

-- cmd is a command to run within a transaction. Commands keep a
-- reference to the previous command's wait channel, in order to
-- enforce an ordering. If a previous wait channel is set, the
-- command waits on it before execution.
local cmd_mt = {}
cmd_mt.__index = cmd_mt
function cmd_mt.new(name, key, endkey, txn_idx, fn)
	local p = setmetatable({}, cmd_mt)
	p:init(name, key, endkey, txn_idx, fn)
	return p
end
function cmd_mt:init(name, key, endkey, txn_idx, fn)
	self.name = name
	self.fn = fn
	self.k = key
	self.ek = endkey
	self.done = false
	self.ev = event.new()
	self.txn_idx = txn_idx
end
function cmd_mt:wait_completion()
	if not self.done then
		print('cmd:exec wait prev', self, self.prev)
		event.wait(nil, self.ev)
	end
end
function cmd_mt:exec(rm, txn)
	if self.prev then
		self.prev:wait_completion()
	end
	print('cmd:exec', self)
	local ok, r = pcall(self.fn, self, rm, txn)
	self.ev:emit('read')
	self.done = true
	if #self.k > 0 and #self.ek > 0 then
		return ("%s%%d.%%d(%s-%s)%s"):format(self.name, self.k, self.ek, self.debug), ok or r
	end
	if #self.k > 0 then
		return ("%s%%d.%%d(%s)%s"):format(self.name, self.k, self.debug), ok or r
	end
	return ("%s%%d.%%d%s"):format(self.name, self.debug), ok or r
end
function cmd_mt:key()
	return ('%d.%s'):format(self.history_idx, self.k)
end
function cmd_mt:endkey()
	return ('%d.%s'):format(self.history_idx, self.ek)
end
function cmd_mt:__tostring()
	if #self.k > 0 and #self.ek > 0 then
		return ("%s%d(%s-%s)"):format(self.name, self.txn_idx, self.k, self.ek)
	end
	if #self.k > 0 then
		return ("%s%d(%s)"):format(self.name, self.txn_idx, self.k)
	end
	return ("%s%d"):format(self.name, self.txn_idx)
end
function cmd_mt:range(rm)
	local k = self:key()
	return rm:find(k, #k)
end
-- readCmd reads a value from the db and stores it in the env.
function cmd_mt:read(rm, txn)
	local v, ts = self:range(rm):get(self:key(), txn)
	if v then
		self.env[self.key] = v
		self.debug = ("[%d ts=%s]"):format(tonumber(v), ts)
	end
end
-- deleteRngCmd deletes the range of values from the db from [key, endKey).
function cmd_mt:delete(rm, txn) 
	self:range(rm):delete(self:key(), txn)
end
-- scanCmd reads the values from the db from [key, endKey).
function cmd_mt:scan(rm, txn)
	local k = self:key()
	local rows = self:range(rm):scan(k, #k, 0, txn)
	local vals = {}
	local keypfx = tostring(self.history_idx).."."
	for _, row in ipairs(rows) do
		local key = rows[1]:sub(#keypfx)
		local v = tonumber(ffi.string(unpack(rows, 3, 4)))
		self.env[key] = v
		table.insert(vals, v)
	end
	c.debug = ("[%s ts=%s]"):format(table.concat(vals, " "), g_last_ts)
end

-- incCmd adds one to the value of c.key in the env and writes
-- it to the db. If c.key isn't in the db, writes 1.
function cmd_mt:inc(rm, txn)
	local k = self:key()
	local r = self:range(rm):merge(k, "1", "inc", txn)
	self.env[self.key] = r
	self.debug = ("[%d ts=%s]"):format(r, g_last_ts)
end

-- sumCmd sums the values of all keys read during the transaction
-- and writes the result to the db.
function cmd_mt:sum(rm, txn)
	local sum = 0
	for k,v in pairs(self.env) do
		sum = sum + v
	end
	print('sum ===> put', sum)
	self:range(rm):put(self:key(), tostring(sum), txn)
	self.debug = ("[%d ts=%s]"):format(sum, g_last_ts)
end

-- commitCmd commits the transaction.
function cmd_mt:commit(rm, txn)
	rm:end_txn(txn, nil, true) -- sync mode
	self.debug = ("[ts=%s]"):format(g_last_ts)
end

-- cmdDict maps from command name to function implementing the command.
-- Use only upper case letters for commands. More than one letter is OK.
local cmdmap = {
	R = cmd_mt.read,
	I = cmd_mt.inc,
	DR = cmd_mt.delete,
	SC = cmd_mt.scan,
	SUM = cmd_mt.sum,
	C = cmd_mt.commit,
}

local cmdRE = "([A-Z]+)%(?([A-Z]*)%-?([A-Z]?)%)?"

function history_string(cmds)
	local ret = {}
	for i=1,#cmds do
		table.insert(ret, tostring(cmds[i]))
	end
	return table.concat(ret, " ")
end

-- parseHistory parses the history string into individual commands
-- and returns a slice.
function parse_history(txn_idx, history)
	-- Parse commands.
	local cmds = {}
	for cmd,key,endkey in history:gmatch(cmdRE) do
		if not cmd then
			exception.raise('fatal', ('failed to parse command %q'):format(elem))
		end
		local fn = cmdmap[cmd]
		if not fn then
			exception.raise('fatal', 'cmd not defined', cmd)
		end
		local c = cmd_mt.new(cmd, key, endkey, txn_idx, fn)
		table.insert(cmds, c)
	end
	return cmds
end

-- parseHistories parses a slice of history strings and returns
-- a slice of command slices, one for each history.
function parse_histories(histories)
	local results = {}
	for i=1,#histories do
		local his = histories[i]
		table.insert(results, parse_history(i, his))
	end
	return results
end

-- Easily accessible slices of transaction isolation variations.
local both_isolations   = {txncoord.SERIALIZABLE, txncoord.SNAPSHOT}
local only_serializable = {txncoord.SERIALIZABLE}
local only_snapshot     = {txncoord.SNAPSHOT}

-- enumerateIsolations returns a slice enumerating all combinations of
-- isolation types across the transactions. The inner slice describes
-- the isolation type for each transaction. The outer slice contains
-- each possible combination of such transaction isolations.
function enumerate_isolations(size, isolations)
	-- Use a count from 0 to pow(# isolations, numTxns) and examine
	-- n-ary digits to get all possible combinations of txn isolations.
	local n = #isolations
	local result = {}
	for i = 1, tonumber(math.pow(n, size)) do
		local pattern = {}
		local val = i
		for j = 1, size do
			pattern[j] = isolations[val%n + 1]
			val = math.floor(val / n)
		end
		table.insert(result, pattern)
	end
	return result
end

test("TestEnumerateIsolations", function ()
	local SSI = txncoord.SERIALIZABLE
	local SI = txncoord.SNAPSHOT
	local isos, exp_isos
	exp_isos = {
		{SI, SSI, SSI},
		{SSI, SI, SSI},
		{SI, SI, SSI},
		{SSI, SSI, SI},
		{SI, SSI, SI},
		{SSI, SI, SI},
		{SI, SI, SI},
		{SSI, SSI, SSI},
	}
	isos = enumerate_isolations(3, both_isolations)
	assert(util.table_equals(isos, exp_isos),"expected match enumeration")
	
	exp_isos = {
		{SSI, SSI, SSI},
	}
	isos = enumerate_isolations(3, only_serializable)
	assert(util.table_equals(isos, exp_isos),"expected match enumeration")
end)



-- enumeratePriorities returns a slice enumerating all combinations of the
-- specified slice of priorities.
function enumerate_priorities(priorities)
	if #priorities <= 1 then
		return {priorities}
	end
	local results = {}
	for i = 1, #priorities do
		local tmp = util.copy_table(priorities)
		local pivot = table.remove(tmp, i)
		local derived = enumerate_priorities(tmp)
		for j = 1, #derived do
			table.insert(derived[j], 1, pivot)
			table.insert(results, derived[j])
		end
	end
	return results
end

test("TestEnumeratePriorities", function ()
	local p1, p2, p3 = 1, 2, 3
	local exp_enum = {
		{p1, p2, p3},
		{p1, p3, p2},
		{p2, p1, p3},
		{p2, p3, p1},
		{p3, p1, p2},
		{p3, p2, p1},
	}
	local enum = enumerate_priorities({p1, p2, p3})
	assert(util.table_equals(enum, exp_enum),"expected match enumeration")
end)

-- enumerateHistories returns a slice enumerating all combinations of
-- collated histories possible given the specified transactions. Each
-- input transaction is a slice of commands. The order of commands for
-- each transaction is stable, but the enumeration provides all
-- possible interleavings between transactions. If symmetric is true,
-- skips exactly N-1/N of the enumeration (where N=len(txns)).
function enumerate_histories(txns, symmetric)
	local results = {}
	local num_txns = #txns
	if symmetric then
		num_txns = 1
	end
	for i=1,num_txns do
		if #txns[i] > 0 then
			local cp = util.copy_table(txns, true)
			table.remove(cp[i], 1)
			local leftover = enumerate_histories(cp, false)
			if #leftover == 0 then
				results = {{txns[i][1]}}
			end
			for j=1,#leftover do
				table.insert(results, {txns[i][1], unpack(leftover[j])})
			end
		end
	end
	return results
end

function make_history_string(hs)
	local t = {}
	for i=1,#hs do
		table.insert(t, tostring(hs[i]))
	end
	return table.concat(t, " ")
end

test("TestEnumerateHistories", function ()
	local txns = parse_histories({"I(A) C", "I(A) C"})
	local enum = enumerate_histories(txns, false)
	local enum_strs = {}
	for i=1,#enum do
		local history = make_history_string(enum[i])
		table.insert(enum_strs, history)
	end
	local enum_symmetric = enumerate_histories(txns, true)
	local enum_symmetric_strs = {}
	for i=1,#enum_symmetric do
		local history = make_history_string(enum_symmetric[i])
		table.insert(enum_symmetric_strs, history)
	end
	local exp_enum_strs = {
		"I1(A) C1 I2(A) C2",
		"I1(A) I2(A) C1 C2",
		"I1(A) I2(A) C2 C1",
		"I2(A) I1(A) C1 C2",
		"I2(A) I1(A) C2 C1",
		"I2(A) C2 I1(A) C1",
	}
	local exp_enum_symmetric_strs = {
		"I1(A) C1 I2(A) C2",
		"I1(A) I2(A) C1 C2",
		"I1(A) I2(A) C2 C1",
	}
	assert(util.table_equals(enum_strs, exp_enum_strs), "expect to match enumeration")
	assert(util.table_equals(enum_symmetric_strs, exp_enum_symmetric_strs), "expect to match enumeration")
end)

-- historyVerifier parses a planned transaction execution history into
-- commands per transaction and each command's previous dependency.
-- When run, each transaction's commands are executed via a goroutine
-- in a separate txn. The results of the execution are added to the
-- actual commands slice. When all txns have completed the actual history
-- is compared to the expected history.
local history_verifier_mt = {}
history_verifier_mt.__index = history_verifier_mt
function history_verifier_mt.new(name, txns, verify, exp_success)
	local p = setmetatable({}, history_verifier_mt)
	p:init(name, txns, verify, exp_success)
	return p
end
function history_verifier_mt:init(name, txns, verify, exp_success)
	self.name = name
	self.txns = parse_histories(txns)
	self.verify = verify
	self.verify_cmds = parse_history(0, verify.history)
	self.exp_success = exp_success
	self.symmetric = self:is_symmetric_history(txns)
end
-- areHistoriesSymmetric returns whether all txn histories are the same.
function history_verifier_mt:is_symmetric_history(txns)
	for i = 1, #txns do
		if txns[i] ~= txns[1] then
			return false
		end
	end
	return true
end
function history_verifier_mt:run(isolations, rm)
	logger.info(("verifying all possible histories for the %q anomaly"):format(self.name))
	local priorities = {}
	for i=1, #self.txns do
		priorities[i] = i
	end
	local enum_pri = enumerate_priorities(priorities)
	local enum_iso = enumerate_isolations(#self.txns, isolations)
	local enum_his = enumerate_histories(self.txns, self.symmetric)

	local history_idx = 1
	local failures = {}
	for _, p in ipairs(enum_pri) do
		for _, i in ipairs(enum_iso) do
			for _, h in ipairs(enum_his) do
				local ok, r = pcall(self.run_history, self, history_idx, p, i, h, rm)
				if not ok then
					logger.report('err', r)
					table.insert(failures, r)
				end
				history_idx = history_idx + 1
			end
		end
	end

	if self.exp_success and #failures > 0 then
		assert(false, ("expected success, experienced %d errors"):format(#failures))
	elseif (not self.exp_success) and #failures == 0 then
		assert(false, ("expected failures for the %q anomaly, but experienced none"):format(self.name))
	end
end
function history_verifier_mt:run_history(his_idx, priorities, isolations, history, rm)
	local planstr = make_history_string(history)
	logger.info("attempting", planstr)--, table.concat(priorities, ":"), table.concat(isolations, ":"))

	local txns = {}
	for i=1,#history do
		local h = history[i]
		if not txns[h.txn_idx] then
			txns[h.txn_idx] = {}
		end
		if i > 1 then
			h.prev = history[i - 1]
			-- print(h, 'waits', history[i - 1])
		end
		table.insert(txns[h.txn_idx], h)
		h.history_idx = his_idx
	end
	self.actual = {}
	local evs = {}
	for i=1,#txns do
		logger.info('start txn sequence', i)
		table.insert(evs, tentacle(function (hv, idx, his)
			hv:run_txn(idx, priorities[idx], isolations[idx], his, rm)
		end, self, i, txns[i]))
	end
	-- wait all txn finished
	event.join(nil, unpack(evs))
	logger.info('finish all txn sequence', #evs)

	-- Construct string for actual history.
	local actual_str = table.concat(self.actual, " ")

	-- Verify history.
	local verify_strs = {}
	local verify_env = {}
	for idx, c in ipairs(self.verify_cmds) do
		c.history_idx = idx
		c.env = verify_env
		local report, err = c:exec(rm, nil)
		if err ~= true then error(err) end
		table.insert(verify_strs, report:format(0, 0))
	end

	self.verify.check(verify_env)
end

function history_verifier_mt:run_txn(txn_idx, priority, isolation, cmds, rm)
	local retry = 0
	local txn_name = ("txn%d"):format(txn_idx)
	assert(txncoord.run_txn({isolation = isolation}, function (txn, hv, pri)
		txn.priority = 1000 - pri

		local env = {}
		-- TODO(spencer): restarts must create additional histories. They
		-- look like: given the current partial history and a restart on
		-- txn txnIdx, re-enumerate a set of all histories containing the
		-- remaining commands from extant txns and all commands from this
		-- restarted txn.

		retry = retry + 1
		if retry >= 2 then
			logger.info(txn_name, "retry", retry)
		end
		for i=1,#cmds do
			cmds[i].env = env
			hv:run_cmd(txn_idx, retry, i, cmds, rm, txn)
		end
	end, self, priority))
end

function history_verifier_mt:run_cmd(txn_idx, retry, cmd_idx, cmds, rm, txn)
	local report, err = cmds[cmd_idx]:exec(rm, txn)
	if err ~= true then error(err) end
	local cmdstr = report:format(txn_idx, retry)
	table.insert(self.actual, cmdstr)
end

-- checkConcurrency creates a history verifier, starts a new database
-- and runs the verifier.
function check_concurrency(name, isolations, txns, verify, exp_success, rm)
	local v = history_verifier_mt.new(name, txns, verify, exp_success)
	v:run(isolations, rm)
end

-- The following tests for concurrency anomalies include documentation
-- taken from the "Concurrency Control Chapter" from the Handbook of
-- Database Technology, written by Patrick O'Neil <poneil@cs.umb.edu>:
-- http://www.cs.umb.edu/~poneil/CCChapter.PDF.
--
-- Notation for planned histories:
--   R(x) - read from key "x"
--   I(x) - increment key "x" by 1
--   SC(x-y) - scan values from keys "x"-"y"
--   SUM(x) - sums all values read during txn and writes sum to "x"
--   C - commit
--
-- Notation for actual histories:
--   Rn.m(x) - read from txn "n" ("m"th retry) of key "x"
--   In.m(x) - increment from txn "n" ("m"th retry) of key "x"
--   SCn.m(x-y) - scan from txn "n" ("m"th retry) of keys "x"-"y"
--   SUMn.m(x) - sums all values read from txn "n" ("m"th retry)
--   Cn.m - commit of txn "n" ("m"th retry)

-- TestTxnDBInconsistentAnalysisAnomaly verifies that neither SI nor
-- SSI isolation are subject to the inconsistent analysis anomaly.
-- This anomaly is also known as dirty reads and is prevented by the
-- READ_COMMITTED ANSI isolation level.
--
-- With inconsistent analysis, there are two concurrent txns. One
-- reads keys A & B, the other reads and then writes keys A & B. The
-- reader must not see intermediate results from the reader/writer.
--
-- Lost update would typically fail with a history such as:
--    R1(A) R2(B) W2(B) R2(A) W2(A) R1(B) C1 C2
test("TestTxnDBInconsistentAnalysisAnomaly", function ()
	local txn1 = "R(A) R(B) SUM(C) C"
	local txn2 = "I(A) I(B) C"
	local verify = {
		history = "R(C)",
		check = function (env)
			print('env[C]', env["C"])
			assert(env["C"] == 2 or env["C"] == 0, ("expected C to be either 0 or 2, got %d"):format(env["C"]))
		end,
	}
	check_concurrency("inconsistent analysis", both_isolations, {txn1, txn2}, verify, true, range_manager)
end)

--[[
// TestTxnDBLostUpdateAnomaly verifies that neither SI nor SSI isolation
// are subject to the lost update anomaly. This anomaly is prevented
// in most cases by using the the READ_COMMITTED ANSI isolation level.
// However, only REPEATABLE_READ fully protects against it.
//
// With lost update, the write from txn1 is overwritten by the write
// from txn2, and thus txn1's update is lost. Both SI and SSI notice
// this write/write conflict and either txn1 or txn2 is aborted,
// depending on priority.
//
// Lost update would typically fail with a history such as:
//   R1(A) R2(A) I1(A) I2(A) C1 C2
//
// However, the following variant will cause a lost update in
// READ_COMMITTED and in practice requires REPEATABLE_READ to avoid.
//   R1(A) R2(A) I1(A) C1 I2(A) C2
func TestTxnDBLostUpdateAnomaly(t *testing.T) {
	txn := "R(A) I(A) C"
	verify := &verifier{
		history: "R(A)",
		checkFn: func(env map[string]int64) error {
			if env["A"] != 2 {
				return util.Errorf("expected A=2, got %d", env["A"])
			}
			return nil
		},
	}
	checkConcurrency("lost update", bothIsolations, []string{txn, txn}, verify, true, t)
}

// TestTxnDBPhantomReadAnomaly verifies that neither SI nor SSI isolation
// are subject to the phantom reads anomaly. This anomaly is prevented by
// the SQL ANSI SERIALIZABLE isolation level, though it's also prevented
// by snapshot isolation (i.e. Oracle's traditional "serializable").
//
// Phantom reads occur when a single txn does two identical queries but
// ends up reading different results. This is a variant of non-repeatable
// reads, but is special because it requires the database to be aware of
// ranges when settling concurrency issues.
//
// Phantom reads would typically fail with a history such as:
//   SC1(A-C) I2(B) C2 SC1(A-C) C1
func TestTxnDBPhantomReadAnomaly(t *testing.T) {
	txn1 := "SC(A-C) SUM(D) SC(A-C) SUM(E) C"
	txn2 := "I(B) C"
	verify := &verifier{
		history: "R(D) R(E)",
		checkFn: func(env map[string]int64) error {
			if env["D"] != env["E"] {
				return util.Errorf("expected first SUM == second SUM (%d != %d)", env["D"], env["E"])
			}
			return nil
		},
	}
	checkConcurrency("phantom read", bothIsolations, []string{txn1, txn2}, verify, true, t)
}

// TestTxnDBPhantomDeleteAnomaly verifies that neither SI nor SSI
// isolation are subject to the phantom deletion anomaly; this is
// similar to phantom reads, but verifies the delete range
// functionality causes read/write conflicts.
//
// Phantom deletes would typically fail with a history such as:
//   DR1(A-C) I2(B) C2 SC1(A-C) C1
func TestTxnDBPhantomDeleteAnomaly(t *testing.T) {
	txn1 := "DR(A-C) SC(A-C) SUM(D) C"
	txn2 := "I(B) C"
	verify := &verifier{
		history: "R(D)",
		checkFn: func(env map[string]int64) error {
			if env["D"] != 0 {
				return util.Errorf("expected delete range to yield an empty scan of same range, sum=%d", env["D"])
			}
			return nil
		},
	}
	checkConcurrency("phantom delete", bothIsolations, []string{txn1, txn2}, verify, true, t)
}

// TestTxnDBWriteSkewAnomaly verifies that SI suffers from the write
// skew anomaly but not SSI. The write skew anamoly is a condition which
// illustrates that snapshot isolation is not serializable in practice.
//
// With write skew, two transactions both read values from A and B
// respectively, but each writes to either A or B only. Thus there are
// no write/write conflicts but a cycle of dependencies which result in
// "skew". Only serializable isolation prevents this anomaly.
//
// Write skew would typically fail with a history such as:
//   SC1(A-C) SC2(A-C) I1(A) SUM1(A) I2(B) SUM2(B)
//
// In the test below, each txn reads A and B and increments one by 1.
// The read values and increment are then summed and written either to
// A or B. If we have serializable isolation, then the final value of
// A + B must be equal to 3 (the first txn sets A or B to 1, the
// second sets the other value to 2, so the total should be
// 3). Snapshot isolation, however, may not notice any conflict (see
// history above) and may set A=1, B=1.
func TestTxnDBWriteSkewAnomaly(t *testing.T) {
	txn1 := "SC(A-C) I(A) SUM(A) C"
	txn2 := "SC(A-C) I(B) SUM(B) C"
	verify := &verifier{
		history: "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if !((env["A"] == 1 && env["B"] == 2) || (env["A"] == 2 && env["B"] == 1)) {
				return util.Errorf("expected either A=1, B=2 -or- A=2, B=1, but have A=%d, B=%d", env["A"], env["B"])
			}
			return nil
		},
	}
	checkConcurrency("write skew", onlySerializable, []string{txn1, txn2}, verify, true, t)
	checkConcurrency("write skew", onlySnapshot, []string{txn1, txn2}, verify, false, t)
}
]]

end)


return true
