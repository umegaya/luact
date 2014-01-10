local ffi = require 'ffi'
local _M = {}

-- cdef
ffi.cdef[[
void *malloc(size_t sz);
]]

-- template cdef
local template_cdef = [[
struct %s_element_t {
	_%s_element_t	*m_prev;
	_%s_element_t	*m_next;
	$				m_data;
};
struct %s_array_t {
	%s_element_t	*m_used;	/* first fullfilled list element */
	%s_element_t	*m_free;	/* first empty list element */
	%s_element_t	*m_pool;	/* first empty list element */
	uint32_t		m_max;	/* max number of allocatable element */
	uint32_t		m_use;	/* number of element in use */
	uint32_t		m_size;	/* size of each allocated chunk */
};
]]

