#!/usr/bin/env python
#coding: utf-8

import traceback
import datetime as dt
from getpass import getpass

from sqlalchemy import create_engine
from flask_script import Manager, Shell

from webapp import app
from utils.common import get_stdout_logger
from gvars import db, notifier, cache
from cache import api_cache
import models
from services.common import AccountService
from services.report import ReportService

manager = Manager(app)
logger = get_stdout_logger('jcyx2-server-manager')


@manager.command
def init_db():
    u""" 初始化数据库 """
    db.create_all()
    print '>>> Tables created!'
    for model_name, records in [
            ('Role', [
                {'name': 'admin', 'descr': u'系统管理员'},
                # {'name': 'company', 'descr': u'公司角色(虚拟)'},
                {'name': 'sdk_dev', 'descr': u'SDK开发者'},
                {'name': 'operator', 'descr': u'运营'},
                {'name': 'customer', 'descr': u'广告主'},
                {'name': 'channel_dev', 'descr': u'开发者'},
            ]),
            # ('User', [
            #     {'role_id': 2, 'login': 'huhulab', 'email': 'huhulab@huhulab.com',
            #      'password': app.config['SECRET_KEY'], 'blocked': True}
            # ])
            ('EventType', [
                {'name': 'show', 'label': u'展示广告', 'descr': u'SDK展示广告'},
                {'name': 'click', 'label': u'点击广告', 'descr': u'用户点击广告'},
                {'name': 'download', 'label': u'下载应用', 'descr': u'用户下载了一个应用'},
                {'name': 'install', 'label': u'安装应用', 'descr': u'用户安装了一个应用'},
                {'name': 'open', 'label': u'打开应用', 'descr': u'用户打开了一个应用'},
            ])
    ]:
        Model = getattr(models, model_name)
        for record in records:
            obj = Model()
            for key, value in record.iteritems():
                print 'key, value:', key, value
                setattr(obj, key, value)
            db.session.add(obj)
            db.session.commit()
            print u'> Added: Model={}, record={}'.format(model_name, record)
    print '>> Init db successfully!'


@manager.command
def create_table(name):
    u""" 创建数据库表 """
    from etc.webapp import SQLALCHEMY_DATABASE_URI

    engine = create_engine(SQLALCHEMY_DATABASE_URI)
    Model = getattr(models, name)
    Model.__table__.create(engine)
    print '>> Table created: {}'.format(Model.__table__)


@manager.option('-r', '--role', default='operator', help=u'角色名(admin/operator/sdk_dev)')
@manager.option('-l', '--login', help=u'登录名')
@manager.option('-e', '--email', default='', help=u'Email 地址')
def add_user(role, login, email):
    u""" 添加用户 """
    if role not in ['admin', 'operator', 'sdk_dev']:
        print u'无法添加据色为 [{}] 的用户'.format(role)
        return
    print u'> Add user: role={}, login={}, email={}'.format(role, login, email)
    the_role = models.Role.query.filter_by(name=role).first()
    user = models.User()
    user.role = the_role
    user.login = login
    user.password = getpass()
    user.email = email
    user.verified = True
    db.session.add(user)
    db.session.commit()
    print '>> User added!!!'


@manager.option('-d', '--date', help=u'日期, 默认:昨天 (格式: %Y-%m-%d)')
@manager.option('-D', '--debug', help=u'Debug 模式: [true, false]')
def record_report(date='', debug='false'):
    u""" 报表入账 """
    if not date:
        yesterday_noon = dt.datetime.now() - dt.timedelta(seconds=3600*36)
        date = yesterday_noon.date()
    else:
        date = dt.datetime.strptime(date, '%Y-%m-%d').date()

    debug = debug in ['true', 'yes', 'y']
    notifier.debug = debug
    logger.info('record_report START: date={}'.format(date))
    try:
        AccountService.generate_customer_consumptions(date)
        logger.info('generate_customer_consumptions: [DONE]')
        AccountService.generate_channel_dev_incomes(date)
        logger.info('generate_channel_dev_incomes: [DONE]')
        AccountService.generate_company_accounts(date)
        logger.info('generate_company_accounts: [DONE]')
        notifier.send('record-report-ok', 'date={}'.format(date))
    except Exception:
        err_stack = traceback.format_exc()
        logger.error(err_stack)
        notifier.send('generate-record-report-error', err_stack)


@manager.option('-d', '--date', help=u'日期, 默认:昨天 (格式: %Y-%m-%d)')
@manager.option('-D', '--debug', help=u'Debug 模式: [true, false]')
def company_accounts(date='', debug='false'):
    u""" 报表入账 """
    if not date:
        yesterday_noon = dt.datetime.now() - dt.timedelta(seconds=3600*36)
        date = yesterday_noon.date()
    else:
        date = dt.datetime.strptime(date, '%Y-%m-%d').date()

    debug = debug in ['true', 'yes', 'y']
    notifier.debug = debug

    try:
        AccountService.generate_company_accounts(date)
        logger.info('generate_company_accounts: [DONE]')
        notifier.send('company-accounts-ok', 'date={}'.format(date))
    except Exception:
        err_stack = traceback.format_exc()
        logger.error(err_stack)
        notifier.send('generate-company-accounts-error', err_stack)


@manager.option('-d', '--date', help=u'日期')
@manager.option('-l', '--log-dir', help=u'日志目录')
@manager.option('-c', '--cache-dir', help=u'缓存存储目录')
@manager.option('-r', '--report', help=u'同时输出: [true, false]')
@manager.option('-D', '--debug', help=u'Debug 模式: [true, false]')
def preprocess_log(date='', log_dir='', cache_dir='', report='false', debug='false'):
    u""" 日志预处理 """
    if not date:
        yesterday_noon = dt.datetime.now() - dt.timedelta(seconds=3600*12)
        date = yesterday_noon.date()
    report = report in ['true', 'yes', 'y']
    debug = debug in ['true', 'yes', 'y']
    notifier.debug = debug
    logger.info(u'date={}, log_dir={}, cache_dir={}, report={}, debug={}'.format(
        date, log_dir, cache_dir, report, debug))
    try:
        _, count = ReportService.preprocess_log(date, log_dir, cache_dir, debug)
        logger.info(u'[DONE]: ReportService.preprocess_log, count={}'.format(count))
        notifier.send('preprocess_log', '[DONE]: count={}'.format(count))
        if report:
            ReportService.make_reports(date, log_dir, cache_dir, debug)
            logger.info('[DONE]: ReportService.make_reports')
            notifier.send('make_reports', '[DONE]')
    except Exception:
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        notifier.send('preprocess_log-error', err_msg)


@manager.option('-d', '--date', help=u'日期')
@manager.option('-l', '--log-dir', help=u'日志存储目录')
@manager.option('-c', '--cache-dir', help=u'缓存存储目录')
@manager.option('-D', '--debug', help=u'Debug 模式: [true, false]')
def make_reports(date='', log_dir='', cache_dir='', debug='false'):
    u""" 从事件数据生成报表 """
    if not date:
        yesterday_noon = dt.datetime.now() - dt.timedelta(seconds=3600*12)
        date = yesterday_noon.date()
    debug = debug in ['true', 'yes', 'y']
    logger.info(u'date={}, log_dir={}, cache_dir={}, debug={}'.format(
        date, log_dir, cache_dir, debug))
    try:
        ReportService.make_reports(date, log_dir, cache_dir, debug)
        logger.info('[DONE]: ReportService.make_reports')
        notifier.send('make_reports', '[DONE]')
    except Exception:
        err_stack = traceback.format_exc()
        logger.error(err_stack)
        notifier.send('make_reports-error', err_stack)


@manager.command
def check_advert_enables():
    u""" 没分钟根据广告的生效时间更新广告列表 """

    now = dt.datetime.now()
    minutes = now.hour * 60 + now.minute
    advert_ids = api_cache.get_advert_enables(minutes)

    last_minute = now - dt.timedelta(seconds=60)
    last_minutes = last_minute.hour * 60 + last_minute.minute
    last_advert_ids = api_cache.get_advert_enables(last_minutes)

    if advert_ids != last_advert_ids:
        cache.delete_memoized(api_cache.get_adverts)
        logger.info('Adverts cache deleted because enable time: \n {} => {} \n {} => {}'.format(
            last_minute.strftime('%H:%M'), now.strftime('%H:%M'), last_advert_ids, advert_ids))


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
