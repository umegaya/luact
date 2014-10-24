local data = debug.getinfo(1)
local root = {data.source:find('@(.+)/.+$')}
package.path = root[3]..'/lib/?.lua;'..package.path
-- print(package.path)