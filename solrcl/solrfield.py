# -*- coding: utf8 -*-

class SOLRField(object):
	def __init__(self, name, type, multi=False, copySources=None):
		self.name = name
		self.type = type
		self.multi = multi
		if copySources is None:
			self.copySources = []
		else:
			self.copySources = copySources

	def isCopy(self):
		return len(self.copySources) > 0

	def check(self, value):
		return self.type.check(value)
