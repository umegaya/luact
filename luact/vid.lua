local _M = {}

local vid_mt
function _M.initialize(mt)
	vid_mt = mt
end

function _M.new(name)
	local path = url:match('[^%+]-%+?[^%+]*://[^/]+(.*)')
	if not path then return nil end
	return setmetatable({url=url,path=path}, vid_mt)
end

return _M
