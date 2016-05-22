#!/usr/bin/env python
#coding: utf-8

import traceback
import datetime as dt
from getpass import getpass

from sqlalchemy import create_engine
from flask_script import Manager, Shell

from webapp import app
from utils.common import get_stdout_logger
from gvars import db, cache
from cache import api_cache
import models


manager = Manager(app)
logger = get_stdout_logger('{xxx}-server-manager')


@manager.command
def init_db():
    u""" 初始化数据库 """
    db.create_all()
    print '>>> Tables created!'


@manager.command
def create_table(name):
    u""" 创建数据库表 """
    from etc.webapp import SQLALCHEMY_DATABASE_URI

    engine = create_engine(SQLALCHEMY_DATABASE_URI)
    Model = getattr(models, name)
    Model.__table__.create(engine)
    print '>> Table created: {}'.format(Model.__table__)


@manager.command
def run(port):
    u""" 运行 Debug 服务器 """
    app.debug = True
    app.run(host='0.0.0.0', port=int(port), debug=True)


def _make_context():
    env = dict(app=app, db=db, models=models)
    for k, v in globals().iteritems():
        if not k.startswith('_') \
           and callable(v) \
           and getattr(v, 'func_code', None) \
           and v.func_code.co_filename == __file__:
            env[k] = v
    return env

manager.add_command("shell", Shell(make_context=_make_context))

if __name__ == '__main__':
    manager.run()
