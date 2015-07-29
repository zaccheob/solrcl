# -*- coding: utf8 -*-

import datetime
import warnings

#Constants
SOLR_DATETIME_FORMAT='%Y-%m-%dT%H:%M:%SZ'


class NotImplementedSOLRTypeWarning(UserWarning): pass


#Utility functions
def solr2datetime(s):
	"""
Utility function to convert SOLR datetime string representation into a datetime python object

:param s: SOLR formatted date to convert
:type x: str
:rtype: datetime.datetime
	"""
	return datetime.datetime.strptime(s, SOLR_DATETIME_FORMAT)

def datetime2solr(d):
	try:
		return unicode(d.strftime(SOLR_DATETIME_FORMAT))
	except AttributeError, e:
		raise ValueError, "Not a datetime compatible object: %s" % repr(d)


class SOLRType(object):
	def __init__(self, name, className):
		self.name =  name
		self.className = className

		#Shortening className for org.apache.solr.schema 
		try:
			base_class_name, class_name = self.className.rsplit('.', 1)
			if base_class_name != 'org.apache.solr.schema':
				class_name = self.className
		except ValueError:
			class_name = self.className

		for action in ('check', 'serialize', 'deserialize'):
			try:
				setattr(self, action, getattr(self, '_%s_%s' % (action, class_name)))
			except AttributeError:
				warnings.warn("Unknown SOLR class %s" % self.className, NotImplementedSOLRTypeWarning)
				setattr(self, action, getattr(self, '_%s_default' % action))

	def check(self, value):
		#Dinamically defined on init
		pass

	def serialize(self, value):
		#Dinamically defined on init
		pass

	def deserialize(self, value):
		#Dinamically defined on init
		pass

	def _check_default(self, value):
		pass

	def _check_BoolField(self, value):
		assert value is True or value is False

	def _check_TextField(self, value):
		assert isinstance(value, unicode)

	def _check_StrField(self, value):
		assert isinstance(value, unicode)

	def _check_TrieDateField(self, value):
		assert isinstance(value, datetime.datetime)

	def _check_TrieIntField(self, value):
		assert isinstance(value, int)
		assert value <= 2147483648

	def _check_TrieLongField(self, value):
		assert isinstance(value, int) or isinstance(value, long)

	def _check_TrieFloatField(self, value):
		assert isinstance(value, float)

	def _serialize_default(self, value):
		return unicode(value)

	def _serialize_BoolField(self, value):
		return {True: u'true', False: u'false'}[value]

	def _serialize_TextField(self, value):
		return value

	def _serialize_StrField(self, value):
		return value

	def _serialize_TrieDateField(self, value):
		return datetime2solr(value)

	def _serialize_TrieIntField(self, value):
		return unicode(value)

	def _serialize_TrieLongField(self, value):
		return unicode(value)

	def _serialize_TrieFloatField(self, value):
		return unicode(value)

	def _deserialize_default(self, value):
		return unicode(value)

	def _deserialize_BoolField(self, value):
		#In this way if value is already a boolean it works. This behaviour is the same as int() float()...
		return { u'true': True, u'false': False, True: True, False: False}[value]

	def _deserialize_TextField(self, value):
		return value

	def _deserialize_StrField(self, value):
		return value

	def _deserialize_TrieDateField(self, value):
		return solr2datetime(value)

	def _deserialize_TrieIntField(self, value):
		return int(value)

	def _deserialize_TrieLongField(self, value):
		return int(value)

	def _deserialize_TrieFloatField(self, value):
		return float(value)
