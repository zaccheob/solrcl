# -*- coding: utf8 -*-
"""Classes representing a SOLR field"""

class SOLRField(object):
    """A SOLR field with all properties from schema"""
    def __init__(self, name, solrtype, multi=False, copySources=None):
        self.name = name
        self.type = solrtype
        self.multi = multi
        if copySources is None:
            self.copySources = []
        else:
            self.copySources = copySources

    def isCopy(self):
        """Returns True if field is a copyfield"""
        return len(self.copySources) > 0

    def check(self, value):
        """Calls the check function for the field type"""
        return self.type.check(value)
