package = 'Lua-ReQL'
version = '0.6-7'

source = {
  url = 'git://github.com/grandquista/Lua-ReQL',
  tag = 'v0.6.7',
}

description = {
  summary = 'A Lua driver for RethinkDB.',
  detailed = 'A Lua driver for RethinkDB.',
  homepage = 'https://github.com/grandquista/Lua-ReQL/wiki',
  maintainer = 'Adam Grandquist <grandquista@gmail.com>',
  license = 'Apache',
}

dependencies = {
  'lua > 5.1',
  'luasocket ~> 3',
  'luajson ~> 1',
}

build = {
  type = 'builtin',
  modules = {
    ['rethinkdb'] = 'src/rethinkdb.lua',
    ['reql.class'] = 'src/reql/class.lua',
    ['reql.cursor'] = 'src/reql/cursor.lua',
    ['reql.errors'] = 'src/reql/errors.lua',
    ['reql.pprint'] = 'src/reql/pprint.lua',
  }
}
