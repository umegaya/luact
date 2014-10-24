--[[
	eg) {
		{"s", "short", "[1-9][0-9]*"},
		{"l", "long", "%w+"},
	}
]]
return function (args, opts_defs, DEBUG)
	local ret = {}
	for i=1,#args,1 do
		local a = args[i]
		local long, short, v, vv, found
		long, v = a:match('^%-%-(%w+)=(.*)$')
		if long then
			for _, def in ipairs(opts_defs) do
				if def[2] == long then
					found = def
					goto on_found
				end
			end
		end
		short = a:match('^%-(%w+)$')
		if short then
			i = i + 1
			v = args[i]
			for _, def in ipairs(opts_defs) do
				if def[1] == short then
					found = def
					goto on_found
				end
			end
		end
::on_found::
		local vv = {found[3] and v:match(found[3]) or v}
		if vv[1] then
			ret[def[2]] = (found[4] and found[4](unpack(vv)) or vv[1])
		end
	end
	if DEBUG then
		for k,v in pairs(ret) do
			print('opts', k, v)
		end
	end
	return ret
end