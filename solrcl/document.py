# -*- coding: utf8 -*-
import warnings
import xml.etree.cElementTree as ET
import re

import logging
logger = logging.getLogger("solrcl")
logger.setLevel(logging.DEBUG)

import exceptions

class SOLRDocumentError(exceptions.SOLRError): pass
class SOLRDocumentWarning(UserWarning): pass

class SOLRDocument(object):
	"""Class that stores data for a SOLR document. To instantiate SOLRDocument from xml use SOLRDocumentFactory"""
	def __init__(self, solrid, solrcore):
		self._fields = {}
		self._child_docs = []
		self.solr = solrcore

		self.setField(self.solr.id_field, solrid)

	def __getattr__(self, name):
		#Shortcut to id field
		if name == "id":
			return self.getField(self.solr.id_field)
		else:
			raise AttributeError

	def __eq__(self, other):
		if type(other) is type(self):
			return set(self._fields.keys()) == set(other._fields.keys()) and all(set(self._fields[x]) == set(other._fields[x]) for x in self._fields.keys()) and sorted(self._child_docs) == sorted(other._child_docs)
		return False

	def __ne__(self, other):
		return not self.__eq__(other)

	def __lt__(self, other):
		#Need it to make the object sortable (sorting is used in __eq__ method to test child documents)
		return self.id < other.id

	def _serializeValue(self, v, encoding='utf8'):
		if isinstance(v, unicode):
			return v
		elif isinstance(v, str):
			return v.decode(encoding)
		else:
			return unicode(v)
			
	def setField(self, fieldname, fieldvalue):
		if self._fields.has_key(fieldname):
			del self._fields[fieldname]

		if isinstance(fieldvalue, list):
			for x in fieldvalue:
				self.appendFieldValue(fieldname, x)
		else:
			self.appendFieldValue(fieldname, fieldvalue)

	def appendFieldValue(self, fieldname, fieldvalue):
		logger.debug("appendFieldValue %s %s" % (repr(fieldname), repr(fieldvalue)))
		if self.solr.fields.has_key(fieldname):
			if fieldvalue is None:
				self._fields.setdefault(fieldname, [])
				self._fields[fieldname].append(fieldvalue)
			else:
				try:
					self.solr.fields[fieldname].type.check(fieldvalue)
				except AssertionError, e:
					raise SOLRDocumentError, "Invalid value %s for field %s (type %s)" % (repr(fieldvalue), fieldname, self.solr.fields[fieldname].type.name)
				self._fields.setdefault(fieldname, [])
				if len(self._fields[fieldname]) > 0 and not self.solr.fields[fieldname].multi:
					raise SOLRDocumentError, "Multiple values for not multivalued field %s" % fieldname
	
				self._fields[fieldname].append(fieldvalue)
		else:
			raise SOLRDocumentError, "Field %s not in schema" % fieldname

	def getField(self, fieldname):
		ret = self._fields[fieldname]
		if self.solr.fields[fieldname].multi:
			return ret
		else:
			return ret[0]

	def removeField(self, fieldname):
		self._fields.pop(fieldname, None)

	def getFieldDefault(self, fieldname, default=None):
		try:
			return self.getField(fieldname)
		except KeyError:
			return default

	def addChild(self, doc):
		self._child_docs.append(doc)

	def getChildDocs(self):
		return self._child_docs

	def hasChildDocs(self):
		return bool(self._child_docs)

	def _toXML(self, update=True):
		doc = ET.Element('doc')
		for field, value in self._fields.iteritems():
			if value[0] is None:
				f = ET.SubElement(doc, 'field', null='true', name=field)
				if field != self.solr.id_field and update:
					f.set('update', 'set')
				f.text = ''
			else:
				for v in value:
					f = ET.SubElement(doc, 'field', null='false', name=field)
					if field != self.solr.id_field and update:
						f.set('update', 'set')
					f.text = self.solr.fields[field].type.serialize(v)

		for child in self._child_docs:
			doc.append(child._toXML(update=update))
		return doc

	def toXML(self, update=True):
		"""Serializes SOLRDocument into an XML string suitable for SOLR update request handler"""
		#Unfortunately it seems there's no way to avoid xml declaration... so I've to remove it with a regexp
		return re.sub(r"^<\?xml version='1.0' encoding='[^']*'\?>\s*", '', ET.tostring(self._toXML(update=update), encoding='utf8'))

	def clone(self):
		#Don't use copy.deepcopy because i don't want to clone also self.solr object
		anotherme = SOLRDocument(self.id, self.solr)
		for fieldname in self._fields.iterkeys():
			anotherme.setField(fieldname, self.getField(fieldname))

		for child in self.getChildDocs():
			anotherme.addChild(child.clone())

		return anotherme

	def update(self, otherdoc, merge_child_docs=True):
		for fieldname in otherdoc._fields.iterkeys():
			self.setField(fieldname, otherdoc.getField(fieldname))

		if not merge_child_docs:
			#Shortcut! A removeChild method would be better, but I'm lazy :)
			self._child_docs = []

		actual_child_docs = dict(((d.id, d) for d in self.getChildDocs()))

		for child_doc in otherdoc.getChildDocs():
			if actual_child_docs.has_key(child_doc.id):
				#update child
				actual_child_docs[child_doc.id].update(child_doc)
			else:
				#new child
				self.addChild(child_doc.clone())
			

class SOLRDocumentFactory(object):
	"""Class with methods to create SOLRDocument instances that fits on solr core"""
	def __init__(self, solr):
		"""Initializes the instance with solr core"""
		self.solr = solr

	def _fromXMLDoc(self, xmldoc):
		id_in_record = False
	
		#Create a new document with a fake id, unfortunately id field is not necessarly in the first position so I should iterate all fields to find it before reading other fields. In this way I can set it later. The counterpart is that I have to enforce that id field exists in another way.
		doc = SOLRDocument(u'changeme', self.solr)
	
		for field in xmldoc:
			if field.tag == 'field':
				fieldname = field.get('name')
				if not self.solr.fields.has_key(fieldname):
					raise SOLRDocumentError, "Field %s does not exist in schema" % fieldname

				if fieldname == self.solr.id_field:
					id_in_record = True

				if field.get('null') == 'true':
					doc.setField(fieldname, None)
				else:
					if fieldname == self.solr.id_field:
						doc.setField(fieldname, self.solr.fields[fieldname].type.deserialize(unicode(field.text)))
					else:
						doc.appendFieldValue(fieldname, self.solr.fields[fieldname].type.deserialize(unicode(field.text)))

			elif field.tag == 'doc':
				doc.addChild(self._fromXMLDoc(field))
			else:
				raise SOLRDocumentError, "Invalid tag {0} in doc".format(field.tag)
		if not id_in_record:
			raise SOLRDocumentError, "Missing unique id field in doc"

		return doc


	def fromXML(self, fh):
		"""Returns a generator over SOLRDocument instances from an xml document read from the file like object fh. Fields are checked against solr schema and if not valid a SOLRDocumentXMLError exception is raised."""
		doc_depth = 0
		for (event, element) in ET.iterparse(fh, events=('start', 'end')):
			if event == 'start':
				if element.tag == 'doc':
					doc_depth += 1
			elif event == 'end':
				if element.tag == 'doc':
					doc_depth -= 1
				elif element.tag in ('field', 'add'):
					pass
				else:
					raise SOLRDocumentError, "Invalid tag {0}".format(element.tag)


			if element.tag == 'doc' and event == 'end' and doc_depth == 0:
				try:
					yield self._fromXMLDoc(element)
				except SOLRDocumentError, e:
					#Transform document errors in warnings to continue to next
					warnings.warn("%s" % e, SOLRDocumentWarning)
