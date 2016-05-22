#coding: utf-8

from werkzeug.exceptions import (
    BadRequest as BadRequestBase,
    Unauthorized as UnauthorizedBase,
    Forbidden as ForbiddenBase)
from flask import make_response


# ==============================================================================
#  Custom exceptions
# ==============================================================================

class InvalidQueryOperator(BadRequestBase):
    def __init__(self, op):
        self.op = op
        data = { 'op': op }
        super(BadRequestBase, self).__init__(self, response=make_response(data))

    def __unicode__(self):
        return u'InvalidQueryOperator(op={})'.format(self.op)


class PageOverflow(BadRequestBase):
    def __init__(self, query_args, offset, total):
        self.query_args = query_args
        self.offset = offset
        self.total = total
        data = {
            'message': u'页码溢出错误!',
            'query_args': query_args,
            'offset': offset,
            'total': total
        }
        super(BadRequestBase, self).__init__(self, response=make_response(data))


class BadRequest(BadRequestBase):
    """ 400 """
    def __init__(self, message=u'请求参数错误'):
        self.message = message
        super(BadRequestBase, self).__init__(self, response=make_response(message))


class Unauthorized(UnauthorizedBase):
    """ 401 """
    def __init__(self, message=u'认证失败'):
        self.message = message
        super(UnauthorizedBase, self).__init__(self, response=make_response(message))


class Forbidden(ForbiddenBase):
    """ 403 """
    def __init__(self, message=u'无权访问'):
        self.message = message
        super(ForbiddenBase, self).__init__(self, response=make_response(message))
