# -*- coding: utf8 -*-

import logging

class baseLogFormatter(logging.Formatter, object):
	default_format = '%(asctime)s [%(core)s on %(domain)s:%(port)s] - %(levelname)s - %(message)s'
	simple_format = '%(asctime)s [%(core)s on %(domain)s:%(port)s] - %(message)s'
	default_format_nocore = '%(asctime)s [%(domain)s:%(port)s] - %(levelname)s - %(message)s'
	simple_format_nocore = '%(asctime)s [%(domain)s:%(port)s] - %(message)s'
	default_format_base = '%(asctime)s - %(levelname)s - %(message)s'
	simple_format_base = '%(asctime)s - %(message)s'

	def __init__(self, fmt=None, datefmt='%Y-%m-%d %H:%M:%S'):
		return super(baseLogFormatter, self).__init__(fmt=fmt, datefmt=datefmt)

	def format(self, record):
		if hasattr(record, 'core'):
			self._fmt = {logging.INFO: self.simple_format}.get(record.levelno, self.default_format)
		elif hasattr(record, 'domain'):
			self._fmt = {logging.INFO: self.simple_format_nocore}.get(record.levelno, self.default_format_nocore)
		else:
			self._fmt = {logging.INFO: self.simple_format_base}.get(record.levelno, self.default_format_base)

		return super(baseLogFormatter, self).format(record)

class extendedLogFormatter(baseLogFormatter):
	default_format = '%(asctime)s [%(core)s on %(domain)s:%(port)s] - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
	simple_format = '%(asctime)s [%(core)s on %(domain)s:%(port)s] - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
	default_format_nocore = '%(asctime)s [%(domain)s:%(port)s] - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
	simple_format_nocore = '%(asctime)s [%(domain)s:%(port)s] - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
	default_format_nocore = '%(asctime)s - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
	simple_format_nocore = '%(asctime)s - %(process)d - %(name)s - %(funcName)s - %(levelname)s - %(message)s'

class httpLogFilter(logging.Filter):
	def filter(self, record):
		if record.funcName == 'request':
			return False
		return True
