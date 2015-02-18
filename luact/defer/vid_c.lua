local _M = (require 'pulpo.package').module('luact.defer.vid_c')

local vid_mt

function _M.initialize_local(mt, vid_opts)
	vid_mt = mt
	return (require ('luact.defer.vid_local')).initialize(vid_opts)
end

function _M.initialize_dist(mt, parent_address, dht_opts, vid_opts)
	vid_mt = mt
	return true --(require ('luact.defer.vid_dist')).initialize(parent_address, dht_opts, vid_opts)
end

function _M.new(url)
	local host, path = url:match('([^%+]-%+?[^%+]*://[^/]+)(.*)')
	if not path then 
		return nil 
	end
	return setmetatable({host=host,path=path}, vid_mt)
end

function _M.debug_getent(url)
	local host, path = url:match('([^%+]-%+?[^%+]*://[^/]+)(.*)')
	if not path then 
		return nil 
	end
	return _M.dht:getent(path)
end

return _M