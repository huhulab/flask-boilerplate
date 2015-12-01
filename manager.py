#!/usr/bin/env python
# coding: utf-8

import os
import copy
import commands
import argparse
import py_compile
import ConfigParser

import toml


# ==============================================================================
#  Global config
# ==============================================================================
PWD = os.environ.get('PWD')
SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = SCRIPT_PATH if os.path.realpath(PWD) != os.path.realpath(SCRIPT_PATH) else ''
DEFAULT_CONFIG_PATH = os.path.join(BASE_PATH, 'deploy/config.toml')
SUPERVISOR_CONFIG_PATH = os.path.join(BASE_PATH, 'deploy/supervisor/supervisord.conf')

class Renderer(object):
    def __init__(self, tmpl_type, root_path=None):
        self.tmpl_type = tmpl_type
        self.root_path = root_path

    def render(self, tmpl_path, args, root_path=None):
        root_path = root_path if root_path else self.root_path
        tmpl_path = os.path.join(root_path, tmpl_path)
        if self.tmpl_type == 'jinja2':
            import jinja2
            with open(tmpl_path) as f:
                tmpl_str= f.read().decode('utf-8')
                t = jinja2.Template(tmpl_str, undefined=jinja2.StrictUndefined)
                return t.render(args)
        elif self.tmpl_type == 'mustache':
            # import pystache
            pass


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action')
    parser_config = subparsers.add_parser('config', help='Config this project')
    parser_supervisor = subparsers.add_parser('supervisor', help='Start supervisor')

    parser_config.add_argument('-l', '--list', action='store_true', help='Just list all target files')
    parser_config.add_argument('-c', '--config', default=DEFAULT_CONFIG_PATH,
                        help='The config file (config.toml)')
    parser_config.add_argument('-d', '--dry', action='store_true', help='Do not write files (dry run)')
    parser_supervisor.add_argument('-l', '--list', action='store_true', help='Just list all programs')
    args = parser.parse_args()
    print 'Args: {}'.format(args)
    print '-' * 60
    return args


def config(args):
    with open(args.config) as f:
        config = toml.load(f)

    config_global = config['global']
    base_args = config.get('args', {})
    root_path = config_global.get('root')
    if not root_path:
        root_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
    tmpl_type = config_global.get('template-type', 'jinja2')
    renderer = Renderer(tmpl_type, root_path)
    print '[Root path] = {}'.format(root_path)
    print '-' * 40

    for file_item in config['files']:
        the_args = copy.copy(base_args)
        tmpl_path = file_item.get('tmpl')
        target_path = file_item.get('target')
        if not target_path:
            target_path = tmpl_path.rsplit('.', 1)[0]
        if not tmpl_path:
            tmpl_path = '{}.{}'.format(target_path, tmpl_type)
        if args.list:
            print '[File]: {}'.format(target_path)
            continue

        target_path = os.path.join(root_path, target_path)
        print 'Rendering... => {}'.format(tmpl_path),
        the_args.update(file_item['args'])
        content = renderer.render(tmpl_path, the_args)
        print ' ~ [Render ok]'
        if not args.dry:
            changed = False
            if os.path.exists(target_path):
                with open(target_path, 'r') as f:
                    if content.encode('utf-8') != f.read():
                        changed = True
            else:
                print '>> Create file: {}'.format(target_path)
                changed = True
            if changed:
                print '>> Write file: {}'.format(target_path)
                with open(target_path, 'w') as f:
                    f.write(content.encode('utf-8'))
                # Check syntax
                if target_path.endswith('.py'):
                    py_compile.compile(target_path)
                    print 'Compiling Python file: {} ~ [ok]'.format(target_path)

    print '=' * 60


def parse_programs(supervisor_path):
    parser = ConfigParser.RawConfigParser()
    parser.read(supervisor_path)

    def get_programs(cfg):
        lst = []
        for section in cfg.sections():
            if section.startswith('program:'):
                lst.append(section.split(':')[1])
        return lst

    def parse_include(path, files):
        lst = []
        cmd = 'cd %s && ls %s' % (path, files)
        output = commands.getoutput(cmd)
        for sub_name in output.split('\n'):
            sub_path = os.path.join(path, sub_name)
            sub_parser = ConfigParser.RawConfigParser()
            sub_parser.read(sub_path)
            lst.extend(get_programs(sub_parser))
        return lst

    programs = get_programs(parser)
    try:
        files = parser.get('include', 'files')
        s_path, s_name = os.path.split(supervisor_path)
        programs.extend(parse_include(s_path, files))
    except ConfigParser.NoSectionError:
        pass

    return programs


def supervisor(args):
    if args.list:
        for program in parse_programs(SUPERVISOR_CONFIG_PATH):
            print '[Program]: {}'.format(program)
    else:
        start_cmd = 'supervisord -c {}'.format(SUPERVISOR_CONFIG_PATH)
        status_cmd = 'supervisorctl -c {} status'.format(SUPERVISOR_CONFIG_PATH)
        ctl_cmd = 'supervisorctl -c {}'.format(SUPERVISOR_CONFIG_PATH)
        output = commands.getoutput(status_cmd)
        if len(output) > 12 and output[-12:] == 'no such file':
            print 'Supervisor not started, starting...'
            os.system(start_cmd)
        os.system(ctl_cmd)


def main():
    args = parse_args()
    globals()[args.action](args)


if __name__ == '__main__':
    main()
