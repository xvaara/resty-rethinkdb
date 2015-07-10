local class = require'reql/class'
local errors = require'reql/errors'

return class(
  'Cursor',
  {
    __init = function(self, conn, token, opts, root)
      self._conn = conn
      self._token = token
      self._opts = opts
      self._root = root -- current query
      self._responses = {}
      self._response_index = 1
    end,
    _add_response = function(self, response)
      local t = response.t
      if not self._type then self._type = response.n or true end
      if response.r[1] or t == --[[Response.WAIT_COMPLETE]] then
        table.insert(self._responses, response)
      end
      if t ~= --[[Response.SUCCESS_PARTIAL]] then
        -- We got an error, SUCCESS_SEQUENCE, WAIT_COMPLETE, or a SUCCESS_ATOM
        self._end_flag = true
        self._conn:_del_query(self._token)
      else
        self._conn:_continue_query(self._token)
      end
      while (self._cb and self._responses[1]) do
        self:_run_cb(self._cb)
      end
    end,
    _run_cb = function(self, callback)
      local cb = function(err, row)
        return callback(err, row)
      end
      local response = self._responses[1]
      -- Behavior varies considerably based on response type
      -- Error responses are not discarded, and the error will be sent to all future callbacks
      local t = response.t
      if t == --[[Response.SUCCESS_ATOM]] or t == --[[Response.SUCCESS_PARTIAL]] or t == --[[Response.SUCCESS_SEQUENCE]] then
        local err

        local status, row = pcall(
          recursively_convert_pseudotype,
          response.r[self._response_index],
          self._opts
        )
        if not status then
          err = row
          row = response.r[self._response_index]
        end

        self._response_index = self._response_index + 1

        -- If we're done with this response, discard it
        if not response.r[self._response_index] then
          table.remove(self._responses, 1)
          self._response_index = 1
        end

        return cb(err, row)
      end
      self:clear()
      if t == --[[Response.COMPILE_ERROR]] then
        return cb(errors.ReQLCompileError(response.r[1], self._root, response.b))
      elseif t == --[[Response.CLIENT_ERROR]] then
        return cb(errors.ReQLClientError(response.r[1], self._root, response.b))
      elseif t == --[[Response.RUNTIME_ERROR]] then
        return cb(errors.ReQLRuntimeError(response.r[1], self._root, response.b))
      elseif t == --[[Response.WAIT_COMPLETE]] then
        return cb()
      end
      return cb(errors.ReQLDriverError('Unknown response type ' .. t))
    end,
    set = function(self, callback)
      self._cb = callback
    end,
    clear = function(self)
      self._cb = nil
    end,
    -- Implement IterableResult
    next = function(self, callback)
      local cb = function(err, row)
        return callback(err, row)
      end
      if self._cb then
        return cb(errors.ReQLDriverError('Use `cur:clear()` before `cur:next`.'))
      end
      -- Try to get a row out of the responses
      while not self._responses[1] do
        if self._end_flag then
          return cb(errors.ReQLDriverError('No more rows in the cursor.'))
        end
        self._conn:_get_response(self._token)
      end
      return self:_run_cb(cb)
    end,
    close = function(self, callback)
      if not self._end_flag then
        self._conn:_end_query(self._token)
        self._end_flag = true
      end
      if callback then return callback() end
    end,
    each = function(self, callback, on_finished)
      if type(callback) ~= 'function' then
        return r._logger('First argument to each must be a function.')
      end
      if on_finished and type(on_finished) ~= 'function' then
        return r._logger('Optional second argument to each must be a function.')
      end
      local cb = function(row)
        return callback(row)
      end
      function next_cb(err, data)
        if err then
          if err.message == 'ReQLDriverError No more rows in the cursor.' then
            err = nil
          end
          if on_finished then
            return on_finished(err)
          end
        else
          cb(data)
          return self:next(next_cb)
        end
      end
      return self:next(next_cb)
    end,
    to_array = function(self, callback)
      if not self._type then self._conn:_get_response(self._token) end
      if type(self._type) == 'number' then
        return cb(errors.ReQLDriverError('`to_array` is not available for feeds.'))
      end
      local cb = function(err, arr)
        return callback(err, arr)
      end
      local arr = {}
      return self:each(
        function(row)
          table.insert(arr, row)
        end,
        function(err)
          return cb(err, arr)
        end
      )
    end,
  }
)
