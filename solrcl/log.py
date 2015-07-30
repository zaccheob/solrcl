# -*- coding: utf8 -*-
"""Utility classes for logging"""

import logging

class BaseLogFormatter(logging.Formatter, object):
    """Log formatter class for standard messages"""
    default_format = '%(asctime)s [%(core)s on %(domain)s:%(port)s] - %(levelname)s - %(message)s'
    simple_format = '%(asctime)s [%(core)s on %(domain)s:%(port)s] - %(message)s'
    default_format_nocore = '%(asctime)s [%(domain)s:%(port)s] - %(levelname)s - %(message)s'
    simple_format_nocore = '%(asctime)s [%(domain)s:%(port)s] - %(message)s'
    default_format_base = '%(asctime)s - %(levelname)s - %(message)s'
    simple_format_base = '%(asctime)s - %(message)s'

    def __init__(self, fmt=None, datefmt='%Y-%m-%d %H:%M:%S'):
        super(BaseLogFormatter, self).__init__(fmt=fmt, datefmt=datefmt)

    def format(self, record):
        if hasattr(record, 'core'):
            self._fmt = {logging.INFO: self.simple_format}.get(record.levelno, self.default_format)
        elif hasattr(record, 'domain'):
            self._fmt = {logging.INFO: self.simple_format_nocore}.get(record.levelno, self.default_format_nocore)
        else:
            self._fmt = {logging.INFO: self.simple_format_base}.get(record.levelno, self.default_format_base)

        return super(BaseLogFormatter, self).format(record)

class ExtendedLogFormatter(BaseLogFormatter):
    """Log formatter class for extended messages (debug)"""
    default_format = '%(asctime)s [%(core)s on %(domain)s:%(port)s] - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
    simple_format = '%(asctime)s [%(core)s on %(domain)s:%(port)s] - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
    default_format_nocore = '%(asctime)s [%(domain)s:%(port)s] - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
    simple_format_nocore = '%(asctime)s [%(domain)s:%(port)s] - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
    default_format_nocore = '%(asctime)s - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
    simple_format_nocore = '%(asctime)s - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'

class HttpLogFilter(logging.Filter):
    """Logging filter that removes http requests logging"""
    def filter(self, record):
        if record.funcName == 'request':
            return False
        return True
