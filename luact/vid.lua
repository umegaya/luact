local _M = {}

local vid_mt
function _M.initialize(mt)
	vid_mt = mt
end

function _M.new(name)
	local src = {url:find('([^%+]-)%+?([^%+]*)://([^/]+)(.*)')}
	if not src[1] then return nil end
	-- 1: protocol, 2: serdes, 3: hostname, 4: path
	return setmetatable(src, vid_mt)
end

return _M
