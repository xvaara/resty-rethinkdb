--- Interface to handle socket timeouts and recoverable errors.
-- @module rethinkdb.internal.socket
-- @author Adam Grandquist
-- @license Apache
-- @copyright Adam Grandquist 2016

local ssl = require('ssl')

local function socket(r, host, port, ssl_params, timeout)
  local raw_socket, init_err = r.tcp()

  if not raw_socket then
    return nil, init_err
  end

  raw_socket:settimeout(timeout)

  local status

  status, init_err = raw_socket:connect(host, port)

  if not status then
    return nil, init_err
  end

  if ssl_params then
    raw_socket, init_err = ssl.wrap(raw_socket, ssl_params)

    if not raw_socket then
      return nil, init_err
    end

    status = false
    while not status do
      status, init_err = raw_socket:dohandshake()
      if init_err == 'closed' then
        return nil, init_err
      end
    end
  end

  local socket_inst = {}

  socket_inst.sink = r.socket.sink('keep-open', raw_socket)

  function socket_inst.source(_r, length)
    return _r.socket.source('by-length', raw_socket, length)
  end

  function socket_inst.close()
    if not ngx and not ssl_params then  --luacheck: globals ngx
      raw_socket:shutdown()
    end
    raw_socket:close()
  end

  return socket_inst
end

return socket
