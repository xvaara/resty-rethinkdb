local class = require'reql/class'
local pprint = require'reql/pprint'

ReQLError = class(
  'ReQLError',
  function(self, msg, term, frames)
    self.msg = msg
    self.message = self.__class.__name .. ' ' .. msg
    if term then
      self.message = self.message .. ' in:\n' .. pprint(term, frames)
    end
  end
)

ReQLServerError = class('ReQLServerError', ReQLError, {})

return {
  ReQLDriverError = class('ReQLDriverError', ReQLError, {}),
  ReQLRuntimeError = class('ReQLRuntimeError', ReQLServerError, {}),
  ReQLCompileError = class('ReQLCompileError', ReQLServerError, {}),
  ReQLClientError = class('ReQLClientError', ReQLServerError, {})
}
