import os
import re
import string

import ReQLprotodef as protodef


def ast_classes(ast_constants, ast_method_names):
    return '\n'.join(
        '{0} = ast({0!r}, {{tt = {1}, st = {2!r}}})'.format(
            name,
            getattr(protodef.Term.TermType, name),
            ast_method_names[name]
        ) for name in ast_constants
    )


def const_args(num):
    args = ', '.join('arg{}'.format(n) for n in range(num))
    return ''.join((
        '{} = function(', args, ', opts) return {}(opts, ', args, ') end'
    ))


def ast_methods(ast_constants, ast_method_names):
    ast_methods_w_opt = dict(
        {
            name: '{} = function(...) return {}(get_opts(...)) end'
            for name in (
                'CIRCLE', 'DELETE', 'DISTINCT', 'EQ_JOIN', 'FILTER', 'GET_ALL',
                'GET_INTERSECTING', 'GET_NEAREST', 'GROUP', 'HTTP',
                'INDEX_CREATE', 'INDEX_RENAME', 'ISO8601', 'JAVASCRIPT',
                'ORDER_BY', 'RANDOM', 'REPLACE', 'SLICE', 'TABLE',
                'TABLE_CREATE', 'UPDATE'
            )
        },
        BETWEEN=const_args(3),
        BETWEEN_DEPRECATED=const_args(3),
        DISTANCE=const_args(2),
        DURING=const_args(3),
        FILTER=const_args(2),
        INSERT=const_args(2),
        UPDATE=const_args(2)
    )
    return ',\n  '.join(
        ast_methods_w_opt.get(
            name, '{} = function(...) return {}({{}}, ...) end'
        ).format(
            ast_method_names[name], name
        ) for name in ast_constants
    )


def ast_names(ast_constants):
    lines = []
    for name in ast_constants:
        if lines and len(lines[-1]) + len(name) < 77:
            lines[-1] += ', {}'.format(name)
        else:
            lines.append('local {}'.format(name))
    return '\n'.join(lines)


ast_constants = sorted(
    term for term in dir(protodef.Term.TermType)
    if not term.startswith('_') and term not in ('DATUM', 'IMPLICIT_VAR')
)

ast_method_names = {name: name.lower() for name in ast_constants}
ast_method_names.update({
    'AND': 'and_', 'BRACKET': 'index', 'ERROR': 'error_', 'FUNCALL': 'do_',
    'JAVASCRIPT': 'js', 'NOT': 'not_', 'OR': 'or_'
})

format_kwargs = {
    'AstClasses': ast_classes(ast_constants, ast_method_names),
    'AstMethods': ast_methods(ast_constants, ast_method_names),
    'AstNames': ast_names(ast_constants),
    'Query': protodef.Query.QueryType,
    'Response': protodef.Response.ResponseType,
    'Term': protodef.Term.TermType,
}


class BuildFormat(string.Formatter):
    fspec = re.compile('--\[\[(.+?)\]\]')

    def parse(self, string):
        last = 0
        for match in self.fspec.finditer(string):
            yield string[last:match.start()], match.group(1), '', 's'
            last = match.end()
        yield string[last:], None, None, None


def process(file_name):
    with open('src/' + file_name + '.pre.lua') as io:
        s = io.read()
    s = BuildFormat().vformat(s, (), format_kwargs)
    with open('src/' + file_name + '.lua', 'w') as io:
        io.write(s)


def main():
    print('building source')

    process('rethinkdb')

    print('building successful')


if __name__ == '__main__':
    main()
