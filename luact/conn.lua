local require_on_boot = (require 'pulpo.package').require
local _M = require_on_boot 'luact.defer.conn_c'

function _M.urlparse(url)
	local proto, serde, address = url:match('([^%+]+)%+?([^%+]*)://(.+)')
	-- print(url, "["..proto.."]", "["..serde.."]", "["..address.."]")
	if not proto then raise('invalid', 'url', url) end
	if #serde <= 0 then serde = _M.DEFAULT_SERDE end
	return proto, serde, address
end
-- self test (temporary!!)
local testproto, testserde, testaddr = "ssh", "json", "my.amazing.service.com:8997"
local testurl = testproto.."+"..testserde.."://"..testaddr
local p,s,a = _M.urlparse(testurl)
assert(p == testproto, "proto should be "..testproto..":"..p)
assert(s == testserde, "serde should be "..testserde..":"..s)
assert(a == testaddr, "proto should be "..testaddr..":"..a)

return _M
