local class = require'reql/class'

function intsp(seq)
  local res = {}
  local sep = ''
  for _, v in ipairs(seq) do
    table.insert(res, {sep, v})
    sep = ', '
  end
  return res
end

function kved(optargs)
  local res = {}
  for k, v in pairs(optargs) do
    table.insert(res, {k, '= ', v})
  end
  return {'{', intsp(res), '}'}
end

function intspallargs(args, optargs)
  local argrepr = {}
  if next(args) then
    table.insert(argrepr, intsp(args))
  end
  if optargs and next(optargs) then
    table.insert(argrepr, kved(optargs))
  end
  return intsp(argrepr)
end

return class(
  'ReQLQueryPrinter',
  {
    __init = function(self, term, frames)
      self.term = term
      self.frames = frames
    end,
    print_query = function(self)
      local carrots
      if next(self.frames) then
        carrots = self:compose_carrots(self.term, self.frames)
      else
        carrots = {self:carrotify(self:compose_term(self.term))}
      end
      carrots = self:join_tree(carrots):gsub('[^%^]', '')
      return self:join_tree(self:compose_term(self.term)) .. '\n' .. carrots
    end,
    compose_term = function(self, term)
      if type(term) ~= 'table' then return '' .. term end
      local args = {}
      for i, arg in ipairs(term.args) do
        args[i] = self:compose_term(arg)
      end
      local optargs = {}
      for key, arg in pairs(term.optargs) do
        optargs[key] = self:compose_term(arg)
      end
      return self.compose(term, args, optargs)
    end,
    compose_carrots = function(self, term, frames)
      local frame = table.remove(frames, 1)
      local args = {}
      for i, arg in ipairs(term.args) do
        if frame == (i - 1) then
          args[i] = self:compose_carrots(arg, frames)
        else
          args[i] = self:compose_term(arg)
        end
      end
      local optargs = {}
      for key, arg in pairs(term.optargs) do
        if frame == key then
          optargs[key] = self:compose_carrots(arg, frames)
        else
          optargs[key] = self:compose_term(arg)
        end
      end
      if frame then
        return self.compose(term, args, optargs)
      end
      return self:carrotify(self.compose(term, args, optargs))
    end,
    carrot_marker = {},
    carrotify = function(self, tree)
      return {carrot_marker, tree}
    end,
    compose = function(self, args, optargs)
      if self.tt == nil then
        if self.data == nil then
          return 'nil'
        end
        return '' .. self.data
      end
      if self.tt == 2 then
        return {
          '{',
          intsp(args),
          '}'
        }
      end
      if self.tt == 3 then
        return kved(optargs)
      end
      if self.tt == 10 then
        return {'var_' .. args[1]}
      end
      if self.tt == 155 and not self.args[1] then
        return 'r.binary(<data>)'
      end
      if self.tt == 170 then
        return {
          args[1],
          '(',
          args[2],
          ')'
        }
      end
      if self.tt == 69 then
        return {
          'function(',
          intsp((function()
            local _accum_0 = {}
            for i, v in ipairs(self.args[1]) do
              _accum_0[i] = 'var_' .. v
            end
            return _accum_0
          end)()),
          ') return ',
          args[2],
          ' end'
        }
      end
      if self.tt == 64 then
        local func = table.remove(args, 1)
        if func then
          table.insert(args, func)
        end
      end
      if not self.args then
        return {
          type(self)
        }
      end
      return {
        'r.' .. self.st .. '(',
        intspallargs(args, optargs),
        ')'
      }
    end,
    join_tree = function(self, tree)
      local str = ''
      for _, term in ipairs(tree) do
        if type(term) == 'table' then
          if #term == 2 and term[1] == self.carrot_marker then
            str = str .. self:join_tree(term[2]):gsub('.', '^')
          else
            str = str .. self:join_tree(term)
          end
        else
          str = str .. term
        end
      end
      return str
    end
  }
)
