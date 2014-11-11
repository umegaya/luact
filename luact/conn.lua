local require_on_boot = (require 'pulpo.package').require
local _M = require_on_boot 'luact.defer.conn_c'

function _M.parse_hostname(hostname)
	local proto, serde, address = hostname:match('([^%+]+)%+?([^%+]*)://(.+)')
	-- print(url, "["..proto.."]", "["..serde.."]", "["..address.."]")
	if not proto then raise('invalid', 'hostname', hostname) end
	if #serde <= 0 then serde = _M.DEFAULT_SERDE end
	local user, credential, addr = address:match('([^@:]+):?([^@]*)@(.+)')
	if user then
		return proto, serde, addr, user, credential
	else
		return proto, serde, address
	end
end
-- self test (temporary!!)
local testproto, testserde, testaddr = "ssh", "json", "my.amazing.service.com:8997"
local testurl = testproto.."+"..testserde.."://"..testaddr
local p,s,a,u,c = _M.parse_hostname(testurl)
assert(p == testproto, "proto should be "..testproto.." => "..p)
assert(s == testserde, "serde should be "..testserde.." => "..s)
assert(a == testaddr, "addr should be "..testaddr.." => "..a)
assert(u == nil, "user should be nil => "..tostring(u))
assert(c == nil, "cred should be nil => "..tostring(c))

local testuser, testcred = "h%40ge", "fu:ga"
local testurl2 = testproto.."+"..testserde.."://"..testuser..":"..testcred.."@"..testaddr
local p,s,a,u,c = _M.parse_hostname(testurl2)
assert(p == testproto, "proto should be "..testproto.." => "..p)
assert(s == testserde, "serde should be "..testserde.." => "..s)
assert(a == testaddr, "addr should be "..testaddr.." => "..a)
assert(u == testuser, "user should be "..testuser.." => "..u)
assert(c == testcred, "cred should be "..testcred.." => "..c)

return _M
