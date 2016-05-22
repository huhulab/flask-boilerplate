#coding: utf-8

import json
import decimal
from datetime import datetime, date

from sqlalchemy.types import TypeDecorator
from flask import current_app

from utils.exceptions import InvalidQueryOperator, PageOverflow
from utils.common import datetime_to_utcts
from gvars import db


class StrippedString(TypeDecorator):
    """ Stripped the string before saving"""

    impl = db.String

    def process_bind_param(self, value, dialect):
        return value.strip() if value else value


class SessionMixin(object):
    """ Common methods for model classes """

    def to_dict(self):
        data = {}
        for c in self.__class__.__table__.columns:
            value = getattr(self, c.name)
            if isinstance(value, decimal.Decimal):
                value = float(value)
            elif isinstance(value, datetime):
                data['{}_str'.format(c.name)] = value.strftime("%Y-%m-%d %H:%M:%S")
                value = datetime_to_utcts(value)
            elif isinstance(value, date):
                data['{}_str'.format(c.name)] = value.strftime("%Y-%m-%d")
                value = datetime_to_utcts(value)
            data[c.name] = value
        return data

    def clone(self):
        Model = type(self)
        obj = Model()
        for c in self.__class__.__table__.columns:
            if c.name not in ['id', 'created_at', 'updated_at']:
                setattr(obj, c.name, getattr(self, c.name))
        return obj

    def __unicode__(self):
        return u'<{}(id={}, json={})>'.format(type(self).__name__, self.id, self.to_dict())

    def __str__(self):
        """ If you want your object pretty printed, just add `__unicode__` to your Model. """
        return unicode(self).encode('utf-8')


class TheBaseModel(db.Model, SessionMixin):
    __abstract__ = True


class BaseModel(TheBaseModel):
    __abstract__ = True

    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.now)
    updated_at = db.Column(db.DateTime, nullable=True, onupdate=datetime.now)


class QueryProcessor():
    FILTER_DICT = {
        # f ==> field;  v ==> value;
        'contains'  : lambda f, v: f.contains(v),
        '~contains' : lambda f, v: ~f.contains(v),
        'ilike'     : lambda f, v: f.ilike(u'%{}%'.format(v)),
        '~ilike'    : lambda f, v: ~f.ilike(u'%{}%'.format(v)),
        'like'      : lambda f, v: f.like(u'%{}%'.format(v)),
        '~like'     : lambda f, v: ~f.like(u'%{}%'.format(v)),
        'in'        : lambda f, v: f.in_(v),
        '~in'       : lambda f, v: ~f.in_(v),
        '=='        : lambda f, v: f == v,
        '!='        : lambda f, v: f != v,
        '>'         : lambda f, v: f > v,
        '>='        : lambda f, v: f >= v,
        '<'         : lambda f, v: f < v,
        '<='        : lambda f, v: f <= v,
    }

    def __init__(self, args, filters=tuple(), sort=tuple(),
                 page=1, perpage=20, model=None, to_dict_kwargs=None):
        self.args = args
        self.filters = filters
        self.sort = sort
        self.page = page
        self.perpage = perpage
        self.model = model
        self.query = model.query
        self.to_dict_kwargs = to_dict_kwargs if to_dict_kwargs else {}

    def resolve(self):
        """
        Steps:
        =====
          1. filter
          1.1 Check total records.
          2. sort
          3. offset
          4. limit
        """
        Model = self.model
        query = self.query
        if query is None:
            raise ValueError('DB Model query not given: {}'.format(str(self)))

        filter_dict = QueryProcessor.FILTER_DICT
        def gen_filter_cond(tModel, name, op, value):
            if op not in filter_dict:
                raise InvalidQueryOperator(op)
            field = getattr(tModel, name)
            filter_func = filter_dict[op]
            return filter_func(field, value)

        filters = self.filters
        orderBy = self.sort
        offset = self.perpage * (self.page - 1)
        limit = self.perpage

        # 1. Filter
        filter_conds = [gen_filter_cond(Model, name, op, value)
                        for name, op, value in filters]
        query = query.filter(db.and_(*filter_conds))
        total = query.count()
        if 0 < total <= offset:
            raise PageOverflow(str(self), offset, total)
        # 2. Sort
        orderBy_conds = [getattr(getattr(Model, name), order)()
                         for name, order in orderBy]
        query = query.order_by(*orderBy_conds)
        # 3. Offset
        query = query.offset(offset)
        # 4. Limit: if limit <=0, then get all records
        if limit > 0:
            query = query.limit(limit)
        return total, query


    def get_rv(self, with_objects=True):
        total, query = self.resolve()
        rv = {'total': total}
        if with_objects:
            # 所以 to_dict 方法只允许给出有名字的参数
            rv['objects'] = [obj.to_dict(**self.to_dict_kwargs) for obj in query.all()]
        return rv


    @staticmethod
    def build(request, model, to_dict_kwargs=None):
        """
        query = {
            'page': Integer,
            'perpage': Integer,
            'filters': [
                [String:field, String:operation, String:value],
                ...
            ],
            'sort': [
                [String:field, String:order],
                ...
            ]
        }
        """
        args = request.args.get('q', '{}')
        args = json.loads(args)

        page    = args.get('page', 1)
        perpage = args.get('perpage', current_app.config['DEFAULT_PERPAGE'])
        filters = args.get('filters', [])
        sort    = args.get('sort', [])

        Meta = getattr(model, 'Meta', object())
        filters = filters or getattr(Meta, 'default_filters', [])
        sort = sort or getattr(Meta, 'default_sort', []) or [["id", "desc"]]
        to_dict_kwargs = to_dict_kwargs if to_dict_kwargs else {}
        return QueryProcessor(args, filters, sort, page, perpage, model, to_dict_kwargs)

    def update_filters(self, callback):
        self.filters = callback(self.filters)

    def update_sort(self, callback):
        self.sort = callback(self.sort)

    def __str__(self): return unicode(self).encode('utf-8')
    def __unicode__(self):
        return u'<QueryProcessor(page={}, perpage={}, filters={}, sort={})>'.format(
            self.page, self.perpage, self.filters, self.sort)
