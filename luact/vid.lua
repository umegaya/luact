local _M = {}

local vid_mt
function _M.initialize(mt)
	vid_mt = mt
end

function _M.new(url)
	local host, path = url:match('([^%+]-%+?[^%+]*://[^/]+)(.*)')
	if not path then return nil end
	return setmetatable({host=host,path=path}, vid_mt)
end

return _M
