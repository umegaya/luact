--[[
	eg) {
		{"s", "short", "[1-9][0-9]*"},
		{"l", "long", "%w+"},
	}
]]
return function (args, opts_defs, DEBUG)
	local ret, others = {}, {}
	opts_defs = opts_defs or {}
	for i=1,#args,1 do
		local a = args[i]
		local long, short, v, vv, found
		long, v = a:match('^%-%-([%w_%.]+)=(.*)$')
		if long then
			for _, def in ipairs(opts_defs) do
				if def[2] == long then
					found = def
					goto on_found
				end
			end
			-- long option always treated as configuration
			if not found then
				ret[long] = v
				goto next
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
		if found then
			local vv = {found[3] and v:match(found[3]) or v}
			if vv[1] then
				if found[4] then
					ret[found[2]] = found[4](unpack(vv))
				else
					ret[found[2]] = vv[1]
				end
			end
		else
			table.insert(others, a)
		end
::next::
	end
	local opts = {}
	for k,v in pairs(ret) do
		local dir = {}
		for token in k:gmatch("[^%.]+") do
			table.insert(dir, token)
		end
		local tmp = opts
		for i=1,#dir do
			local k = dir[i]
			if i < #dir then
				if not tmp[k] then
					tmp[k] = {}
				end
				tmp = tmp[k]
			else
				tmp[k] = v
			end
		end
	end
	return opts, others
end





