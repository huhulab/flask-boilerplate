#coding: utf-8

import json

from werkzeug.http import HTTP_STATUS_CODES
from werkzeug.exceptions import HTTPException
from werkzeug.wrappers import Response as ResponseBase
# from werkzeug.routing import RequestRedirect
from flask import Flask as FlaskBase
from flask import request, current_app
from flask.views import MethodView
import IP

from gvars import statsd_client
from utils.model import QueryProcessor
from utils.exceptions import PageOverflow


class BaseMethodView(MethodView):
    # The blueprint of this view
    blueprint = None

    # The endpoint name
    endpoint = ''

    # Url rules:
    #   API: http://flask.pocoo.org/docs/0.10/api/#flask.Flask.url_map
    #   Example: http://flask.pocoo.org/docs/0.10/views/
    url_rules = []

    # Default data model (subclass of Flask-SQLAlchemy.Model)
    Model = None

    @classmethod
    def register_urls(cls, bp=None):
        """ Call this classmethod before register blueprints to flask app:

            def register_blueprints(app, bps):
                for view_cls in BaseMethodView.__subclasses__():
                   view_cls.register_urls()
                for bp in bps:
                   app.register_blueprint(bp)
       """
        view = cls.as_view(cls.endpoint)
        if bp is None:
            bp = cls.blueprint
        for args, kwargs in cls.url_rules:
            kwargs.setdefault('view_func', view)
            bp.add_url_rule(*args, **kwargs)

    def get_list(self):
        """ Default method for get a page of objects  """
        return QueryProcessor.build(request, self.Model)

    # FIXME: For permission controls
    def get_one(self, oid):
        """ Get object by id (primary key)"""
        obj = self.Model.query.get_or_404(oid)
        return obj.to_dict()

    def get(self, oid):
        if oid is None:
            return self.get_list()
        else:
            return self.get_one(oid)


class MyResponse(ResponseBase):
    default_mimetype = 'application/json'


class MyFlask(FlaskBase):
    response_class = MyResponse

    def make_response(self, rv):
        status = headers = None
        if isinstance(rv, tuple):
            rv, status, headers = rv + (None,) * (3 - len(rv))
        current_app.logger.debug(u'Response(original): rv={}, status={}, headers={}'.format(
            rv, status, headers))

        if isinstance(rv, HTTPException):
            message = rv.response.data if rv.response else HTTP_STATUS_CODES[rv.code]
            status = rv.code
            rv = {'message': message}
        elif isinstance(rv, QueryProcessor):
            try:
                rv = rv.get_rv()
            except PageOverflow as e:
                status = e.code
                rv = {'message': e.message}

        # Dump return data as JSON string.
        if isinstance(rv, (dict, list, tuple)):
            rv = json.dumps(rv)

        if status >= 400:
            statsd_client.incr('{}.endpint.{}.{}'.format(
                status, request.endpoint, request.method))

        current_app.logger.debug(u'Response(final): rv={}, status={}, headers={}'.format(
            rv, status, headers))
        return FlaskBase.make_response(self, (rv, status, headers))
