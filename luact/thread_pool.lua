local ffi = require 'ffiex'
local thread = require 'thread'
local util = require 'util'
local _M = {}
local C = ffi.C

-- cdefs
ffi.cdef[[
typedef struct _thread_task_t {
	char *proc;
	void *args;
	_thread_task_t *next;
} thread_task_t;

typedef struct {
	int n_tasks;
	pthread_mutex_t mtx[1];
	thread_task_t *first, *last;
} thread_queue_t;

typedef struct {
	thread_handle_t th;
	thread_queue_t queue;
	bool alive;
} thread_state_t;

typedef struct {
	thread_state_t *states;
	int used[1];
	int limit;
} thread_pool_t;

typedef struct {
	thread_pool_t *pool;
	char *proc;
	void *args;
} thread_launch_args_t;
]]

-- metatype
ffi.metatype("thread_queue_t", {
	__index = {
		init = function (self)
			self.n_tasks = 0
			C.pthread_mutex_init(self.mtx)
			self.first = C.NULL
			self.last = C.NULL
		end,
		fin = function (self)
			local tmp = self.first
			C.pthread_mutex_lock(self.mtx)
			while tmp ~= C.NULL do
				local nextp = tmp.next
				C.free(tmp)
				tmp = nextp
			end
			self.first = C.NULL
			self.last = C.NULL
			self.n_task = 0
			C.pthread_mutex_unlock(self.mtx)
			C.pthread_mutex_destroy(self.mtx)
		end,
		push = function (self, proc, args)
			local task = ffi.cast("thread_task_t*", C.malloc(ffi.sizeof("thread_task_t"))
			task.proc = proc
			task.args = args
			C.pthread_mutex_lock(self.mtx)
			self.last.next = task 
			self.last = task
			if self.last == C.NULL then
				self.last = task
				self.first = task
			else
				self.last.next = task
				self.last = task
			end
			self.n_task = (self.n_task + 1)
			C.pthread_mutex_unlock(self.mtx)
		end,
		pop = function (self)
			local task
			C.pthread_mutex_lock(self.mtx)
			if self.first == C.NULL then
				assert(self.n_task == 0)
				task = nil
			else
				task = self.first 
				self.first = task.next
				if task == self.last then
					assert(task.next == C.NULL)
					self.last = C.NULL
				end
				self.n_task = (self.n_task - 1)
			end
			C.pthread_mutex_unlock(self.mtx)
			return task
		end,
	}
})


-- local variable shared with all thread in same process
local thread_pool
local current --> current thread


-- methods
-- set limitation of # of cpu core usage.
_M.init = function (limit, pool)
	limit = (limit or util.n_cpu())
	local states = ffi.cast("thread_state_t*", C.malloc(ffi.sizeof("thread_state_t") * limit))
	ffi.fill(states, ffi.sizeof("thread_state_t") * limit)
	thread_pool = ffi.cast("thread_pool_t*", C.malloc(ffi.sizeof("thread_pool_t"))
	thread_pool.states = states
	thread_pool.used[0] = 0
	thread_pool.limit = limit
end

-- do not call this. it raises error if you call it
_M.init_with_pool = function (pool) 
	assert(thread_pool == nil)
	thread_pool = pool
	current = pool.states[util.sync_fetch_add(pool.used, 1)]
	current.th = thread.me
	current.queue = ffi.cast("thread_queue_t*", C.malloc(ffi.sizeof("thread_queue_t")))
	current.queue:init()
	current.alive = thread_queue_t
end
_M.pool = function ()
	assert(thread_pool)
	return thread_pool
end

-- get fresh thread from pool. 
-- before reach to core-usage limit, it create new thread every time user called it.
-- after reach to limit, it chooses lowest load thread. (currently lowest # of actor)
-- currently, proc must not be closure and args must be allocated by C.malloc.
_M.get = function (proc, args)
	local a = ffi.cast("thread_launch_args_t*", C.malloc(ffi.sizeof("thread_launch_args_t")))
	local proc = string.dump(proc)
	a.pool = thread_pool
	a.proc = C.malloc(#proc)
	ffi.copy(a.proc, proc, #proc)
	a.args = ffi.cast("void*", args)
	if thread_pool.used < thread_pool.limit then
		return thread.create(function (argp)
			local ffi = require 'ffiex'
			local thread_pool = require 'thread_pool'
			local targs = ffi.cast("thread_launch_args_t*", argp)
			thread_pool.init_with_pool(targs.pool)
			current.queue:push(targs.proc, targs.args)
			ffi.C.free(targs)
			while current.alive do
				local task = current.queue:pop()
				if task then
					local proc,args = load(task.proc),task.args
					ffi.C.free(task.proc)
					ffi.C.free(task)
					proc(args)
					ffi.C.free(args)
				end
			end
			return ffi.C.NULL
		end, a)
	else
		local lowest = nil
		for i=0,threads.used - 1,1 do
			if (not lowest) or (lowest.queue.n_task > threads.states[i].queue.n_task) then
				lowest = threads.states[i]
			end
		end
		assert(lowest)
		lowest.queue:push(a.proc, a.args)
		ffi.C.free(a)
		return lowest
	end
end

-- get current thread handle.
-- if nil is returned, it means current thread is not luact worker.
_M.current = function ()
	return current and current.id or nil
end
_M.is_worker = _M.current

-- get current thread alive.
_M.alive = function ()
	return current and current.alive or true
end

-- terminate specified (if null, current) thread
-- joined thread must check _M.alive and do necessary shutdown when alive will be false.
_M.join = function (t)
	if t then
		if _M.is_worker() and thread.equal(t, current.th) then
			assert(false, "cannot join current thread")
		end
		for i=0,thread_pool.used - 1,1 do
			if thread.equal(t, thread_pool.states[i].th) then
				thread_pool.states[i].alive = false
				break
			end
		end
	elseif not _M.is_worker() then
		for i=0,thread_pool.used - 1,1 do
			thread_pool.states[i].alive = false
		end		
		for i=0,thread_pool.used - 1,1 do
			thread.join(thread_pool.states[i].th)
		end		
	end
	thread.join(t)
end
_M.fin = function ()
	_M.join()
end

return _M
