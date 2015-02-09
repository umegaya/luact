local fn = require 'luact.optparse'

local args = {
	"--datadir=/tmp/hoge",
	"main.lua",
	"--foo.bar.baz=fuga",
}

local r, o = fn(args)

assert(r.datadir == "/tmp/hoge")
assert(r.foo.bar.baz == "fuga")
assert(#o == 1 and o[1] == "main.lua")

local args2 = {
	"-c", "hogehoge+garbage",
	"-fbb", "bot.but",
	"-w", "false",
}

local r2 = fn(args2, {
	{"c", "datadir", "^[%w]+", function (m) return "/tmp/"..m end },
	{"fbb", "foo.bar.baz"},
	{"w", "watch", "%w+", function (m)
		if m == "false" then return false end
		return tonumber(m)
	end}
})

assert(r2.datadir == "/tmp/hogehoge")
assert(r2.foo.bar.baz == "bot.but")
assert(r2.watch == false)

return true
