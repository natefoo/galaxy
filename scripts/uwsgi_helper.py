from __future__ import print_function

import os
import sys

from six import string_types
from six.moves import shlex_quote

GALAXY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(1, os.path.join(GALAXY_ROOT, 'lib'))

from galaxy.util.path import get_ext
from galaxy.util.properties import load_app_properties, nice_config_parser
from galaxy.util.script import main_factory


DESCRIPTION = "Script to determine uWSGI command line arguments"
# socket is not an alias for http, but it is assumed that if you configure a socket in your uwsgi config you do not
# want to run the default http server (or you can configure it yourself)
ALIASES = {
    'virtualenv': ('home', 'venv', 'pyhome'),
    'pythonpath': ('python-path', 'pp'),
    'http': ('httprouter', 'socket', 'uwsgi-socket', 'suwsgi-socket', 'ssl-socket'),
}
DEFAULT_ARGS = {
    '_all_': ('chdir', 'virtualenv', 'pythonpath', 'threads', 'http', 'static-map', 'die-on-term', 'hook-master-start', 'enable-threads'),
    'galaxy': ('py-call-osafterfork', 'offload-threads', 'route'),
    'reports': (),
    'tool_shed': (),
}
DEFAULT_PORTS = {
    'galaxy': 8080,
    'reports': 9001,
    'tool_shed': 9009,
}

GIE_PROXY_CONFIG = """uwsgi:
    master: true
    sessions: {sessions}
    protocol: http
    http-websockets: true
    offload-threads: 2
    python-raw: /home/nate/work/galaxy/lib/galaxy/web/proxy/uwsgi/uwsgi_proxy_funcs.py
    route-run: rpcvar:TARGET_HOST dynamic_proxy_mapper ${{HTTP_HOST}} ${{cookie[galaxysession]}}
    route-run: log:Proxy ${{HTTP_HOST}} to ${{TARGET_HOST}}
    route-run: httpdumb:${{TARGET_HOST}}
"""


def __arg_set(arg, kwargs):
    if arg in kwargs:
        return True
    for alias in ALIASES.get(arg, ()):
        if alias in kwargs:
            return True
    return False


def __add_arg(args, arg, value):
    if isinstance(value, bool):
        if value is True:
            args.append((arg, True))
    elif isinstance(value, string_types) or isinstance(value, int):
        args.append((arg, str(value)))
    else:
        [__add_arg(args, arg, v) for v in value]


def __add_config_file_arg(args, config_file, app):
    ext = None
    if config_file:
        ext = get_ext(config_file)
    config_file = os.path.abspath(config_file)
    if ext in ('yaml', 'json'):
        __add_arg(args, ext, config_file)
    elif ext == 'ini':
        config = nice_config_parser(config_file)
        has_logging = config.has_section('loggers')
        if config.has_section('app:main'):
            # uWSGI does not have any way to set the app name when loading with paste.deploy:loadapp(), so hardcoding
            # the name to `main` is fine
            __add_arg(args, 'ini-paste' if not has_logging else 'ini-paste-logged', config_file)
            return  # do not add --module
        else:
            __add_arg(args, ext, config_file)
            if has_logging:
                __add_arg(args, 'paste-logger', True)
    __add_arg(args, 'module', 'galaxy.webapps.{app}.buildapp:uwsgi_app()'.format(app=app))


def __process_args(cliargs, kwargs):
    # it'd be nice if we didn't have to reparse here but we need things out of more than one section
    config_file = cliargs.config_file or kwargs.get('__file__')
    uwsgi_kwargs = load_app_properties(config_file=config_file, config_section='uwsgi')
    defaults = {
        'chdir': GALAXY_ROOT,
        'virtualenv': os.environ.get('VIRTUAL_ENV', './.venv'),
        'pythonpath': 'lib',
        'threads': '4',
        'offload-threads': 2,
        'http': 'localhost:{port}'.format(port=DEFAULT_PORTS[cliargs.app]),
        'static-map': ('/static/style={here}/static/style/blue'.format(here=os.getcwd()),
                       '/static={here}/static'.format(here=os.getcwd())),
        'route': '^/plugins/([^/]+)/([^/]+)/static/(.*)$ static:{here}/config/plugins/$1/$2/static/$3'.format(here=os.getcwd()),
        'die-on-term': True,
        'enable-threads': True,
        'hook-master-start': ('unix_signal:2 gracefully_kill_them_all',
                              'unix_signal:15 gracefully_kill_them_all'),
        'py-call-osafterfork': True,
    }
    args = []
    for arg in DEFAULT_ARGS['_all_'] + DEFAULT_ARGS[cliargs.app]:
        if not __arg_set(arg, uwsgi_kwargs):
            __add_arg(args, arg, defaults[arg])
    __add_config_file_arg(args, config_file, cliargs.app)
    return args


def __vassals_dir(kwargs):
    return kwargs.get('vassals_dir', os.path.join(kwargs.get('data_dir', 'database'), 'vassals'))


def _get_args(cliargs, kwargs):
    if kwargs.get('dynamic_proxy') == 'uwsgi':
        print('--emperor {vassals_dir} --emperor-on-demand-extension .socket'.format(vassals_dir=__vassals_dir(kwargs)))
    else:
        args = []
        for arg in __process_args(cliargs, kwargs):
            optarg = '--%s' % arg
            if val is True:
                args.append(optarg)
            # the = in --optarg=value is usually, but not always, optional
            if value.startswith('='):
                args.append(shlex_quote(optarg + value))
            else:
                args.append(optarg)
                args.append(shlex_quote(value))
        print(' '.join(args))


def _write_vassals(cliargs, kwargs):
    # TODO: some log messages in here showing what files were written would be useful
    if kwargs.get('dynamic_proxy') == 'uwsgi':
        proxy_bind = kwargs.get('dynamic_proxy_bind_ip', '')
        proxy_port = kwargs.get('dynamic_proxy_bind_port', '8800')
        sessions = os.path.abspath(kwargs.get('dynamic_proxy_session_map', os.path.join(kwargs.get('data_dir', 'database'), 'session_map.sqlite')))
        vassals_dir = __vassals_dir(kwargs)
        # can't use yaml dump here because uWSGI's yaml is not valid yaml
        if not os.path.exists(vassals_dir):
            os.makedirs(vassals_dir)
        with open(os.path.join(vassals_dir, 'galaxy.yml'), 'w') as fh:
            fh.write('uwsgi:\n')
            for arg in __process_args(cliargs, kwargs):
                if isinstance(arg[1], bool):
                    arg = (arg[0], str(arg[1]).lower())
                fh.write('    ' + ': '.join(arg) + '\n')
        with open(os.path.join(vassals_dir, 'gie_proxy.yml.socket'), 'w') as fh:
            fh.write('%s:%s\n' % (proxy_bind, proxy_port))
        with open(os.path.join(vassals_dir, 'gie_proxy.yml'), 'w') as fh:
            fh.write(GIE_PROXY_CONFIG.format(sessions=sessions))


def _gen_configs(cliargs, kwargs):
    """This will generate a galaxy.yml and appropriate vassal configs in the current directory for editing"""
    raise NotImplementedError()


ACTIONS = {
    "get_args": _get_args,
    "write_vassals": _write_vassals,
    "gen_configs": _gen_configs,
}


if __name__ == '__main__':
    main = main_factory(description=DESCRIPTION, actions=ACTIONS, default_action="get_uwsgi_args")
    main()
