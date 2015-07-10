local errors = require'reql/errors'

function is_instance(obj, cls, ...)
  if cls == nil then return false end

  if type(cls) == 'string' then
    if type(obj) == cls then
      return true
    end
  elseif type(cls) == 'table' then
    cls = cls.__name
  else
    return false
  end

  if type(obj) == 'table' then
    local obj_cls = obj.__class
    while obj_cls do
      if obj_cls.__name == cls then
        return true
      end
      obj_cls = obj_cls.__parent
    end
  end

  return is_instance(obj, ...)
end

return {
  get_opts = function(...)
    local args = {...}
    local opt = {}
    local pos_opt = args[#args]
    if (type(pos_opt) == 'table') and (not is_instance(pos_opt, 'ReQLOp')) then
      opt = pos_opt
      args[#args] = nil
    end
    return opt, unpack(args)
  end,
  bytes_to_int = function(str)
    local t = {str:byte(1,-1)}
    local n = 0
    for k = 1, #t do
      n = n + t[k] * 2 ^ (8 * k - 8)
    end
    return n
  end,
  int_to_bytes = function(num, bytes)
    local res = {}
    local mul = 0
    for k = bytes, 1, -1 do
      local den = 2 ^ (8 * k - 8)
      res[k] = math.floor(num / den)
      num = math.fmod(num, den)
    end
    return string.char(unpack(res))
  end,
  is_instance = is_instance
}
