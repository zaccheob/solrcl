# -*- coding: utf8 -*-
"""Classes representing SOLR types"""

import re
import datetime
import warnings
import logging

logger = logging.getLogger("solrcl")
logger.setLevel(logging.DEBUG)

#Constants

SOLR_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class NotImplementedSOLRTypeWarning(UserWarning):
	"""UserWarning raised when when a not implemented SOLR type is found in schema"""

#Utility functions
def solr2datetime(s):
    """
Utility function to convert SOLR datetime string representation into a datetime python object

:param s: SOLR formatted date to convert
:type x: str
:rtype: datetime.datetime
    """
    return datetime.datetime.strptime(s, SOLR_DATETIME_FORMAT)

#This function supporting datetime formatting for dates prior to 1-1-1900 comes from
#http://stackoverflow.com/questions/1526170/formatting-date-string-in-python-for-dates-prior-to-1900

ANCIENT_FMT_DICT = {
    '%Y': '{0.year}',
    '%m': '{0.month:02}',
    '%d': '{0.day:02}',
    '%H': '{0.hour:02}',
    '%M': '{0.minute:02}',
    '%S': '{0.second:02}'}

def ancient_fmt(ts, fmt):
    fmt = fmt.replace('%%', '%')
    fmt = re.sub('(%\w)', r'{\1}', fmt)
    return fmt.format(**ANCIENT_FMT_DICT).format(ts)

def datetime2solr(d):
    try:
        if d.year < 1900:
            return unicode(ancient_fmt(d, SOLR_DATETIME_FORMAT))
        else:
            return unicode(d.strftime(SOLR_DATETIME_FORMAT))
    except AttributeError, e:
        raise ValueError, "Not a datetime compatible object: %s" % repr(d)


class SOLRType(object):
    """Class representing a SOLR type that provides methods to check,
 serialize and deserialize values"""
    def __init__(self, name, className):
        self.name = name
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

    @staticmethod
    def check(self, value):
        #Dinamically defined on init
        pass
 
    @staticmethod
    def serialize(self, value):
        #Dinamically defined on init
        pass

    @staticmethod
    def deserialize(self, value):
        #Dinamically defined on init
        pass

    @staticmethod
    def _check_default(value):
        pass

    @staticmethod
    def _check_BoolField(value):
        assert value is True or value is False

    @staticmethod
    def _check_TextField(value):
        assert isinstance(value, unicode)

    @staticmethod
    def _check_StrField(value):
        assert isinstance(value, unicode)

    @staticmethod
    def _check_TrieDateField(value):
        assert isinstance(value, datetime.datetime)

    @staticmethod
    def _check_TrieIntField(value):
        assert isinstance(value, int)
        assert value <= 2147483648

    @staticmethod
    def _check_TrieLongField(value):
        assert isinstance(value, int) or isinstance(value, long)

    @staticmethod
    def _check_TrieFloatField(value):
        assert isinstance(value, float)

    @staticmethod
    def _serialize_default(value):
        return unicode(value)

    @staticmethod
    def _serialize_BoolField(value):
        return {True: u'true', False: u'false'}[value]

    @staticmethod
    def _serialize_TextField(value):
        return value

    @staticmethod
    def _serialize_StrField(value):
        return value

    @staticmethod
    def _serialize_TrieDateField(value):
        return datetime2solr(value)

    @staticmethod
    def _serialize_TrieIntField(value):
        return unicode(value)

    @staticmethod
    def _serialize_TrieLongField(value):
        return unicode(value)

    @staticmethod
    def _serialize_TrieFloatField(value):
        return unicode(value)

    @staticmethod
    def _deserialize_default(value):
        return unicode(value)

    @staticmethod
    def _deserialize_BoolField(value):
        #In this way if value is already a boolean it works.
        #This is the same behaviour as int() float()...
        logger.debug("deserializing %s" % repr(value))
        if isinstance(value, unicode):
            value = value.lower()
        try:
            return{u'true': True, u'false': False, True: True, False: False}[value]
        except KeyError:
            raise ValueError("Invalid value %s for boolean" % repr(value))

    @staticmethod
    def _deserialize_TextField(value):
        return value

    @staticmethod
    def _deserialize_StrField(value):
        return value

    @staticmethod
    def _deserialize_TrieDateField(value):
        return solr2datetime(value)

    @staticmethod
    def _deserialize_TrieIntField(value):
        return int(value)

    @staticmethod
    def _deserialize_TrieLongField(value):
        return int(value)

    @staticmethod
    def _deserialize_TrieFloatField(value):
        return float(value)
