#!/usr/bin/env python
# -*- coding: utf8 -*-
import logging
import warnings

import unittest
import mock
import copy

import datetime
import xml.etree.cElementTree as ET
import StringIO
import requests
import httplib

import solrcl


class TestSolrlibSOLRDocumentBase(unittest.TestCase):
	def setUp(self):
		#Setup a mock SOLRCore instance
		solr = mock.Mock(spec=solrcl.SOLRCore)

		solrtype = mock.Mock(spec=solrcl.SOLRType)
		solrtype.name = 'testtype'
		solrtype.check = mock.Mock()
		solrtype.check.return_value = True
		solrtype.serialize.side_effect = lambda x: x
		solrtype.deserialize.side_effect = lambda x: x

		solrfield = mock.Mock(spec=solrcl.SOLRField)
		solrfield.name = 'myidfield'
		solrfield.type = solrtype
		solrfield.multi = False
		solr.fields = {'myidfield': solrfield}

		solrfield = mock.Mock(spec=solrcl.SOLRField)
		solrfield.name = 'testfield'
		solrfield.type = solrtype
		solrfield.multi = False
		solr.fields['testfield'] = solrfield

		solr.types = {'testtype': solrtype}

		solrfield = mock.Mock(spec=solrcl.SOLRField)
		solrfield.name = 'testfieldmulti'
		solrfield.type = solrtype
		solrfield.multi = True
		solr.fields['testfieldmulti'] = solrfield

		solr.id_field = 'myidfield'

		self.solr = solr


class TestSolrlibSOLRDocument(TestSolrlibSOLRDocumentBase):
	def test_init(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		self.assertEqual(d.id, u'a')
		self.assertEqual(d.getField('myidfield'), u'a')

	def test_setField(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfield', u'b')
		self.assertEqual(d.getField('testfield'), u'b')

	def test_setField_list(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfieldmulti', [u'a', u'b'])
		self.assertEqual(d.getField('testfieldmulti'), [u'a', u'b'])

	def test_setField_None(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfield', None)
		self.assertEqual(d.getField('testfield'), None)

	def test_appendFieldValue_single(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.appendFieldValue('testfield', u'b')
		self.assertEqual(d.getField('testfield'), u'b')

	def test_appendFieldValue_multi(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.appendFieldValue('testfieldmulti', u'b')
		d.appendFieldValue('testfieldmulti', u'c')
		self.assertEqual(d.getField('testfieldmulti'), [u'b', u'c'])

	def test_appendFieldValue_multi2(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfieldmulti', u'b')
		d.appendFieldValue(u'testfieldmulti', u'c')
		self.assertEqual(d.getField('testfieldmulti'), [u'b', u'c'])

	def test_setField_multi_value_in_non_multi_field(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		self.assertRaises(solrcl.SOLRDocumentError, d.setField, 'testfield', [u'a', u'b'])

	def test_appendFieldValue_multi_value_in_non_multi_field(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfield', u'b')
		self.assertRaises(solrcl.SOLRDocumentError, d.appendFieldValue, 'testfield', u'b')

	def test_setField_nonexisting(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		self.assertRaises(solrcl.SOLRDocumentError, d.setField, 'nonexistingfield', u'b')

	def test_appendFieldValue_nonexisting(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		self.assertRaises(solrcl.SOLRDocumentError, d.appendFieldValue, 'nonexistingfield', u'b')

	def test_setField_invalid_value(self):
		#Change the check mockup in order to raise exception
		d = solrcl.SOLRDocument(u'a', self.solr)
		self.solr.types['testtype'].check.side_effect = AssertionError
		self.assertRaises(solrcl.SOLRDocumentError, d.setField, 'testfield', u'b')

	def test_getField_nonexisting(self):
		d = solrcl.SOLRDocument(u"A", self.solr)
		self.assertRaises(KeyError, d.getField, "testfield")

	def test_getFieldDefault(self):
		d = solrcl.SOLRDocument(u"A", self.solr)
		self.assertEqual(d.getFieldDefault("testfield"), None)
		self.assertEqual(d.getFieldDefault("testfield", 'mydefaultvalue'), 'mydefaultvalue')

	def test_removeField(self):
		d = solrcl.SOLRDocument(u"A", self.solr)
		d.setField('testfield', u'a')
		self.assertEqual(d.getField("testfield"), u"a")
		d.removeField("testfield")
		self.assertRaises(KeyError, d.getField, "testfield")
		#If called on nonexisting field should not raise error
		d.removeField("thisfielddoesnotexists")

	def test_getChildDocs(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		d.addChild(dchild)
		self.assertEqual(d.getChildDocs(), [dchild])

	def test_hasChildDocs(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		d.addChild(dchild)
		self.assertTrue(d.hasChildDocs())

		d = solrcl.SOLRDocument(u'a', self.solr)
		self.assertFalse(d.hasChildDocs())
	

	def test_equal(self):
		d1 = solrcl.SOLRDocument(u'a', self.solr)
		d2 = solrcl.SOLRDocument(u'a', self.solr)
		#Set fields in different order: documents must be anyway the same
		d1.setField('testfield', None)
		d1.setField('testfieldmulti', u'b')
		d1.appendFieldValue('testfieldmulti', 'c')
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		dchild.setField('testfieldmulti', [u'à€2', u'à€1'])
		d1.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.2', self.solr)
		dchild.setField('testfieldmulti', [u'à€2', u'à€1'])
		d1.addChild(dchild)

		d2.setField('testfield', None)
		d2.setField('testfieldmulti', u'c')
		d2.appendFieldValue('testfieldmulti', 'b')
		dchild = solrcl.SOLRDocument(u'a.2', self.solr)
		dchild.setField('testfieldmulti', [u'à€1', u'à€2'])
		d2.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		dchild.setField('testfieldmulti', [u'à€1', u'à€2'])
		d2.addChild(dchild)

		self.assertEqual(d1, d2)

	def test_notequal(self):
		d1 = solrcl.SOLRDocument(u'a', self.solr)
		d2 = solrcl.SOLRDocument(u'b', self.solr)
		for d in (d1, d2):
			d.setField('testfieldmulti', u'b')
			d.appendFieldValue('testfieldmulti', 'c')
			dchild = solrcl.SOLRDocument(u'a.1', self.solr)
			d.addChild(dchild)

		self.assertNotEqual(d1, d2)

	def _checkField(self, xmldoc, fieldname, fieldvalues):
		if not isinstance(fieldvalues, list):
			fieldvalues = [fieldvalues]

		foundvalues = []
		for field in xmldoc:
			if field.tag == 'field':
				if field.attrib['name'] == fieldname:
					foundvalues.append(field.text)

		foundvalues.sort()
		fieldvalues.sort()
		self.assertEqual(foundvalues, fieldvalues)

	def _checkFieldAttribute(self, xmldoc, fieldname, attname, attvalue):
		foundattvalues = []
		for field in xmldoc:
			if field.tag == 'field':
				if field.attrib['name'] == fieldname:
					try:
						foundattvalues.append(field.attrib[attname])
					except KeyError:
						pass

		if len(foundattvalues) == 1:
			self.assertEqual(foundattvalues[0], attvalue)

		elif len(foundattvalues) == 0:
			self.assertEqual(None, attvalue)
		else:
			#len > 1, multiple fields, cannot check
			raise ValueError, "Cannot check attribute values for multiple fields"

	def _checkXMLDoc(self, xmlstring):
		#Checks that it doesn't have XML declaration
		self.assertTrue(xmlstring.startswith('<doc'))
		xmldoc = ET.fromstring(xmlstring)
		self.assertEqual(xmldoc.tag, 'doc')
		return xmldoc
		
	def test_toXML(self):
		d = solrcl.SOLRDocument(u'à€', self.solr)
		xml = d.toXML()
		xmldoc = self._checkXMLDoc(xml)
		self._checkField(xmldoc, 'myidfield', u'à€')
		self._checkFieldAttribute(xmldoc, 'myidfield', 'null', 'false')
		self._checkFieldAttribute(xmldoc, 'myidfield', 'update', None)

	def test_toXML_Nonefield(self):
		d = solrcl.SOLRDocument('a', self.solr)
		d.setField('testfield', None)
		xml = d.toXML()

		xmldoc = self._checkXMLDoc(xml)
		self._checkFieldAttribute(xmldoc, 'testfield', 'null', 'true')
		self._checkFieldAttribute(xmldoc, 'testfield', 'update', 'set')

	def test_toXML_multifield(self):
		d = solrcl.SOLRDocument('a', self.solr)
		d.setField('testfieldmulti', [u'à€b', u'à€a'])
		xml = d.toXML()

		xmldoc = self._checkXMLDoc(xml)
		self._checkField(xmldoc, 'testfieldmulti', [u'à€a', u'à€b'])

	def test_toXML_update(self):
		d = solrcl.SOLRDocument('a', self.solr)
		d.setField('testfield', u"a value")
		xml = d.toXML(update=False)
		xmldoc = self._checkXMLDoc(xml)
		self._checkFieldAttribute(xmldoc, 'testfield', 'update', None)

	def test_clone(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfield', None)
		d.setField('testfieldmulti', u'b')
		d.appendFieldValue('testfieldmulti', 'c')
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		dchild.setField('testfieldmulti', [u'à€2', u'à€1'])
		d.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.2', self.solr)
		dchild.setField('testfieldmulti', [u'à€2', u'à€1'])
		d.addChild(dchild)

		clone = d.clone()
		self.assertEqual(d, clone)

	def test_update(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfield', None)
		d.setField('testfieldmulti', u'b')
		d.appendFieldValue('testfieldmulti', 'c')
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		dchild.setField('testfieldmulti', [u'à€2', u'à€1'])
		d.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.2', self.solr)
		dchild.setField('testfieldmulti', [u'à€2', u'à€1'])
		d.addChild(dchild)
		d1 = d

		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfieldmulti', None)
		dchild = solrcl.SOLRDocument(u'a.3', self.solr)
		dchild.setField('testfieldmulti', [u'à€3', u'à€3'])
		d.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		dchild.setField('testfieldmulti', u'changed')
		d.addChild(dchild)
		d2 = d

		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfield', None)
		d.setField('testfieldmulti', None)
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		dchild.setField('testfieldmulti', u"changed")
		d.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.2', self.solr)
		dchild.setField('testfieldmulti', [u'à€2', u'à€1'])
		d.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.3', self.solr)
		dchild.setField('testfieldmulti', [u'à€3', u'à€3'])
		d.addChild(dchild)
		dcheck = d

		d2check = d2.clone()

		d1.update(d2)
		self.assertEqual(d1, dcheck)
		#Verify that d2 has not changed
		self.assertEqual(d2, d2check)

	def test_update_no_merge(self):
		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfield', None)
		d.setField('testfieldmulti', u'b')
		d.appendFieldValue('testfieldmulti', 'c')
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		dchild.setField('testfieldmulti', [u'à€2', u'à€1'])
		d.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.2', self.solr)
		dchild.setField('testfieldmulti', [u'à€2', u'à€1'])
		d.addChild(dchild)
		d1 = d

		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfieldmulti', None)
		dchild = solrcl.SOLRDocument(u'a.3', self.solr)
		dchild.setField('testfieldmulti', [u'à€3', u'à€3'])
		d.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		dchild.setField('testfieldmulti', u'changed')
		d.addChild(dchild)
		d2 = d

		d = solrcl.SOLRDocument(u'a', self.solr)
		d.setField('testfield', None)
		d.setField('testfieldmulti', None)
		dchild = solrcl.SOLRDocument(u'a.1', self.solr)
		dchild.setField('testfieldmulti', u"changed")
		d.addChild(dchild)
		dchild = solrcl.SOLRDocument(u'a.3', self.solr)
		dchild.setField('testfieldmulti', [u'à€3', u'à€3'])
		d.addChild(dchild)
		dcheck = d

		d2check = d2.clone()

		d1.update(d2, merge_child_docs=False)
		self.assertEqual(d1, dcheck)
		#Verify that d2 has not changed
		self.assertEqual(d2, d2check)

class TestSolrlibSOLRDocumentFactory(TestSolrlibSOLRDocumentBase):
	def setUp(self):
		super(TestSolrlibSOLRDocumentFactory, self).setUp()
		self.df = solrcl.SOLRDocumentFactory(self.solr)

	def test_fromXML(self):
		XML = '<doc><field name="testfieldmulti">à€1</field><field name="testfieldmulti">à€2</field><field name="myidfield">a</field><field name="testfield" null="true"></field><doc><field name="myidfield">a.1</field></doc><doc><field name="myidfield">a.2</field><field name="testfield">à€</field></doc></doc>'
		fh = StringIO.StringIO(XML)
		doc = self.df.fromXML(fh).next()

		checkdoc = solrcl.SOLRDocument(u'a', self.solr)
		checkdoc.setField('testfield', None)
		checkdoc.setField('testfieldmulti', [u'à€1', u'à€2'])

		checkdocchild = solrcl.SOLRDocument(u'a.1', self.solr)
		checkdoc.addChild(checkdocchild)

		checkdocchild = solrcl.SOLRDocument(u'a.2', self.solr)
		checkdocchild.setField('testfield', u'à€')
		checkdoc.addChild(checkdocchild)

		self.assertEqual(doc, checkdoc)

	def test_fromXML_multipleDocs(self):
		XML = '<add><doc><field name="myidfield">1</field></doc><doc><field name="myidfield">2</field></doc></add>'
		fh = StringIO.StringIO(XML)
		checkdoc = solrcl.SOLRDocument(u'1', self.solr)
		iterdocs = self.df.fromXML(fh)
		doc = iterdocs.next()
		self.assertEqual(doc, checkdoc)

		checkdoc = solrcl.SOLRDocument(u'2', self.solr)
		doc = iterdocs.next()
		self.assertEqual(doc, checkdoc)

	def test_fromXML_notwellformed(self):
		XML = '<doc><field>this is not xml</doc>'
		fh = StringIO.StringIO(XML)
		iterdocs = self.df.fromXML(fh)
		self.assertRaises(SyntaxError, iterdocs.next)

	def test_fromXML_wrongcharset(self):
		XML = u'<doc><field name="myidfield">à</field></doc>'.encode('latin1')
		fh = StringIO.StringIO(XML)
		iterdocs = self.df.fromXML(fh)
		self.assertRaises(SyntaxError, iterdocs.next)

	def test_fromXML_1wrongField(self):
		XML = '<add><doc><field name="myidfield">1</field><field name="nonexistingfield">aaa</field></doc><doc><field name="myidfield">2</field></doc></add>'
		fh = StringIO.StringIO(XML)
		iterdocs = self.df.fromXML(fh)

		
		with warnings.catch_warnings(record=True) as w:
			warnings.simplefilter('always')
			#Only second document is yielded
			checkdoc = solrcl.SOLRDocument(u'2', self.solr)
			doc = iterdocs.next()
			self.assertEqual(doc, checkdoc)
			self.assertEqual(len(w), 1)
			self.assertEqual(w[0].category, solrcl.SOLRDocumentWarning)

	def test_fromXML_invalidTag(self):
		XML = '<add><doc><field name="myidfield">1</field><invalidtag>aaa</invalidtag></doc></add>'
		fh = StringIO.StringIO(XML)
		iterdocs = self.df.fromXML(fh)

		self.assertRaises(solrcl.SOLRDocumentError, iterdocs.next)

	def test_fromXML_missingId(self):
		XML = '<add><doc><field name="testfield">testvalue</field></doc></add>'
		fh = StringIO.StringIO(XML)
		iterdocs = self.df.fromXML(fh)

		with warnings.catch_warnings(record=True) as w:
			warnings.simplefilter('always')
			for _ in iterdocs:
				pass
			self.assertEqual(len(w), 1)
			self.assertEqual(w[0].category, solrcl.SOLRDocumentWarning)

class TestSOLRType(unittest.TestCase):
	def test_init_BoolField(self):
		t = solrcl.SOLRType('testbool', 'org.apache.solr.schema.BoolField')
		self.assertEqual(t.name, 'testbool')
		self.assertEqual(t.check, t._check_BoolField)
		self.assertEqual(t.serialize, t._serialize_BoolField)

	def test_init_TextField(self):
		t = solrcl.SOLRType('testtext', 'org.apache.solr.schema.TextField')
		self.assertEqual(t.name, 'testtext')
		self.assertEqual(t.check, t._check_TextField)
		self.assertEqual(t.serialize, t._serialize_TextField)

	def test_init_TrieDateField(self):
		t = solrcl.SOLRType('testdate', 'org.apache.solr.schema.TrieDateField')
		self.assertEqual(t.name, 'testdate')
		self.assertEqual(t.check, t._check_TrieDateField)
		self.assertEqual(t.serialize, t._serialize_TrieDateField)

	def test_init_TrieDateField(self):
		t = solrcl.SOLRType('testfloat', 'org.apache.solr.schema.TrieFloatField')
		self.assertEqual(t.name, 'testfloat')
		self.assertEqual(t.check, t._check_TrieFloatField)
		self.assertEqual(t.serialize, t._serialize_TrieFloatField)

	def test_init_StrField(self):
		t = solrcl.SOLRType('teststr', 'org.apache.solr.schema.StrField')
		self.assertEqual(t.name, 'teststr')
		self.assertEqual(t.check, t._check_StrField)
		self.assertEqual(t.serialize, t._serialize_StrField)

	def test_init_TrieIntField(self):
		t = solrcl.SOLRType('testint', 'org.apache.solr.schema.TrieIntField')
		self.assertEqual(t.name, 'testint')
		self.assertEqual(t.check, t._check_TrieIntField)
		self.assertEqual(t.serialize, t._serialize_TrieIntField)

	def test_init_TrieLongField(self):
		t = solrcl.SOLRType('testlong', 'org.apache.solr.schema.TrieLongField')
		self.assertEqual(t.name, 'testlong')
		self.assertEqual(t.check, t._check_TrieLongField)
		self.assertEqual(t.serialize, t._serialize_TrieLongField)

	def test_init_unknown(self):
		with warnings.catch_warnings(record=True) as w:
			warnings.simplefilter('always')
			t = solrcl.SOLRType('testunknown', 'UnknownTypeField')
			self.assertEqual(t.name, 'testunknown')
			self.assertEqual(t.check, t._check_default)
			self.assertEqual(t.serialize, t._serialize_default)
			self.assertEqual(len(w), 3)
			self.assertEqual(w[0].category, solrcl.NotImplementedSOLRTypeWarning)
			self.assertEqual(w[1].category, solrcl.NotImplementedSOLRTypeWarning)
			self.assertEqual(w[2].category, solrcl.NotImplementedSOLRTypeWarning)

	def test_init_unknown2(self):
		with warnings.catch_warnings():
			warnings.resetwarnings()
			warnings.simplefilter('ignore')
			t = solrcl.SOLRType('testunknown', 'this.path.is.unknown.TextField')
			self.assertEqual(t.name, 'testunknown')
			self.assertEqual(t.check, t._check_default)
			self.assertEqual(t.serialize, t._serialize_default)


class TestSOLRTypeMethods(unittest.TestCase):
	def setUp(self):
		with warnings.catch_warnings():
			warnings.simplefilter('ignore')
			self.t = solrcl.SOLRType('fake', 'FakeField')

	def test_check_BoolField(self):
		self.t._check_BoolField(True)
		self.assertRaises(AssertionError, self.t._check_BoolField, 'a')

	def test_check_TextField(self):
		self.t._check_TextField(u'abc')
		self.assertRaises(AssertionError, self.t._check_TextField, 'a')

	def test_check_StrField(self):
		self.t._check_StrField(u'abc')
		self.assertRaises(AssertionError, self.t._check_StrField, 'a')

	def test_check_TrieDateField(self):
		self.t._check_TrieDateField(datetime.datetime.now())
		self.t._check_TrieDateField(datetime.datetime(1850, 1, 1))
		self.assertRaises(AssertionError, self.t._check_TrieDateField, 'a')

	def test_check_TrieIntField(self):
		self.t._check_TrieIntField(1)
		self.assertRaises(AssertionError, self.t._check_TrieIntField, 'a')
		self.assertRaises(AssertionError, self.t._check_TrieIntField, 3000000000)

	def test_check_TrieLongField(self):
		self.t._check_TrieLongField(10)
		self.t._check_TrieLongField(100000000000000000000000000)
		self.assertRaises(AssertionError, self.t._check_TrieIntField, 'a')

	def test_check_TrieFloatField(self):
		self.t._check_TrieFloatField(1.2)
		self.assertRaises(AssertionError, self.t._check_TrieFloatField, 'a')

	def test_serialize_BoolField(self):
		self.assertEqual(self.t._serialize_BoolField(True), 'true')
		self.assertEqual(self.t._serialize_BoolField(False), 'false')

	def test_serialize_TextField(self):
		self.assertEqual(self.t._serialize_TextField(u'aà€'), u'aà€')

	def test_serialize_StrField(self):
		self.assertEqual(self.t._serialize_StrField(u'aà€'), u'aà€')

	def test_serialize_TrieDateField(self):
		self.assertEqual(self.t._serialize_TrieDateField(datetime.datetime(1975, 3, 4, 3, 15, 23)), '1975-03-04T03:15:23Z')
		self.assertEqual(self.t._serialize_TrieDateField(datetime.datetime(1850, 1, 1, 14, 35, 0)), '1850-01-01T14:35:00Z')

	def test_serialize_TrieIntField(self):
		self.assertEqual(self.t._serialize_TrieIntField(123), '123')

	def test_serialize_TrieLongField(self):
		self.assertEqual(self.t._serialize_TrieLongField(1234567890123456), '1234567890123456')

	def test_serialize_TrieFloatField(self):
		self.assertEqual(self.t._serialize_TrieFloatField(1.2345), '1.2345')

	def test_deserialize_BoolField(self):
		self.assertEqual(self.t._deserialize_BoolField(u'true'), True)
		self.assertEqual(self.t._deserialize_BoolField(u'false'), False)

	def test_deserialize_TextField(self):
		self.assertEqual(self.t._deserialize_TextField(u'aà€'), u'aà€')

	def test_deserialize_StrField(self):
		self.assertEqual(self.t._deserialize_StrField(u'aà€'), u'aà€')

	def test_deserialize_TrieDateField(self):
		self.assertEqual(self.t._deserialize_TrieDateField(u'1975-03-04T03:15:23Z'), datetime.datetime(1975, 3, 4, 3, 15, 23))
		self.assertEqual(self.t._deserialize_TrieDateField(u'1850-01-01T14:35:00Z'), datetime.datetime(1850, 1, 1, 14, 35, 0))

	def test_deserialize_TrieIntField(self):
		self.assertEqual(self.t._deserialize_TrieIntField(u'123'), 123)

	def test_deserialize_TrieLongField(self):
		self.assertEqual(self.t._deserialize_TrieLongField(u'1234567890123456'), 1234567890123456)

	def test_deserialize_TrieFloatField(self):
		self.assertEqual(self.t._deserialize_TrieFloatField(u'1.2345'), 1.2345)



class TestSOLRField(unittest.TestCase):
	def test_init(self):
		solrtype = mock.Mock()
		f = solrcl.SOLRField('testfield', solrtype)

		self.assertEqual(f.name, 'testfield')
		self.assertEqual(f.type, solrtype)
		self.assertFalse(f.multi)
		self.assertEqual(f.copySources, [])

	def test_init_multi(self):
		solrtype = mock.Mock()
		f = solrcl.SOLRField('testfield', solrtype, multi=True)

		self.assertEqual(f.name, 'testfield')
		self.assertEqual(f.type, solrtype)
		self.assertTrue(f.multi)
		self.assertEqual(f.copySources, [])

	def test_init_copySources(self):
		solrtype = mock.Mock()
		field1 = mock.Mock()
		f = solrcl.SOLRField('testfield', solrtype, copySources=[field1])

		self.assertEqual(f.name, 'testfield')
		self.assertEqual(f.type, solrtype)
		self.assertFalse(f.multi)
		self.assertEqual(f.copySources, [field1])


class TestSOLRFieldMethods(unittest.TestCase):
	def setUp(self):
		self.solrtype = mock.Mock()
		check = mock.Mock()
		self.solrtype.check = check
		self.f = solrcl.SOLRField('fake', self.solrtype)

	def test_isCopy(self):
		self.assertFalse(self.f.isCopy())
		field = mock.Mock()
		self.f.copySources.append(field)
		self.assertTrue(self.f.isCopy)

	def test_check(self):
		self.f.check('testvalue')
		self.solrtype.check.assert_called_once_with('testvalue')



class TestSOLRRequest(unittest.TestCase):
	def setUp(self):
		self.solr = solrcl.SOLRRequest(domain='localhost', port=8983)

	@mock.patch('requests.get')
	def test_request_default(self, mock_requests_get):
		SOLR_RESPONSE = {'responseHeader': {'status': 0, 'QTime': 5}, 'foo': 'bar'}
		response = mock.Mock()
		response.headers = {'content-type': 'application/json'}
		response.json = mock.Mock()
		response.json.return_value = SOLR_RESPONSE
		mock_requests_get.return_value = response

		solr_response = self.solr.request('foo/bar')
		mock_requests_get.assert_called_with('http://localhost:8983/solr/foo/bar', params={'wt': 'json'}, headers={}, data=None)
		self.assertEqual(solr_response, SOLR_RESPONSE)

	@mock.patch('requests.get')
	def test_requests_parameters(self, mock_requests_get):
		SOLR_RESPONSE = {'responseHeader': {'status': 0, 'QTime': 5}, 'foo': 'bar'}
		response = mock.Mock()
		response.headers = {'content-type': 'application/json'}
		response.json = mock.Mock()
		response.json.return_value = SOLR_RESPONSE
		mock_requests_get.return_value = response

		solr_response = self.solr.request('foo/bar', parameters={'foo': 'bar'})
		mock_requests_get.assert_called_with('http://localhost:8983/solr/foo/bar', params={'wt': 'json', 'foo': 'bar'}, headers={}, data=None)
		self.assertEqual(solr_response, SOLR_RESPONSE)

	@mock.patch('requests.post')
	def test_requests_data(self, mock_requests_post):
		SOLR_RESPONSE = {'responseHeader': {'status': 0, 'QTime': 5}, 'foo': 'bar'}
		response = mock.Mock()
		response.headers = {'content-type': 'application/json'}
		response.json = mock.Mock()
		response.json.return_value = SOLR_RESPONSE
		mock_requests_post.return_value = response

		mydata = mock.Mock()
		solr_response = self.solr.request('foo/bar', data=mydata)
		mock_requests_post.assert_called_with('http://localhost:8983/solr/foo/bar', params={'wt': 'json'}, headers={'Content-type': 'text/xml'}, data=mydata)
		self.assertEqual(solr_response, SOLR_RESPONSE)

	@mock.patch('requests.post')
	def test_requests_data_mimetype(self, mock_requests_post):
		SOLR_RESPONSE = {'responseHeader': {'status': 0, 'QTime': 5}, 'foo': 'bar'}
		response = mock.Mock()
		response.headers = {'content-type': 'application/json'}
		response.json = mock.Mock()
		response.json.return_value = SOLR_RESPONSE
		mock_requests_post.return_value = response

		mydata = mock.Mock()
		solr_response = self.solr.request('foo/bar', data=mydata, dataMIMEType='application/pdf')
		mock_requests_post.assert_called_with('http://localhost:8983/solr/foo/bar', params={'wt': 'json'}, headers={'Content-type': 'application/pdf'}, data=mydata)
		self.assertEqual(solr_response, SOLR_RESPONSE)


	@mock.patch('requests.post')
	def test_requests_NetworkError(self, mock_requests_post):
		mock_requests_post.side_effect = requests.ConnectionError
		mydata = mock.Mock()
		self.assertRaises(solrcl.SOLRNetworkError, self.solr.request, 'foo/bar', data=mydata)
		mock_requests_post.assert_called_with('http://localhost:8983/solr/foo/bar', params={'wt': 'json'}, headers={'Content-type': 'text/xml'}, data=mydata)

	@mock.patch('requests.post')
	def test_requests_SOLRResponseError(self, mock_requests_post):
		response = mock.Mock()
		response.headers = {'content-type': 'text/html'}
		response.raise_for_status = mock.Mock()
		response.raise_for_status.side_effect = requests.RequestException
		response.status_code = 500
		mock_requests_post.return_value = response

		mydata = mock.Mock()
	
		self.assertRaises(solrcl.SOLRResponseError, self.solr.request, 'foo/bar', data=mydata)
		mock_requests_post.assert_called_with('http://localhost:8983/solr/foo/bar', params={'wt': 'json'}, headers={'Content-type': 'text/xml'}, data=mydata)

	@mock.patch('requests.post')
	def test_requests_SOLRResponseFormatError(self, mock_requests_post):
		response = mock.Mock()
		response.headers = {'content-type': 'application/json'}
		response.json = mock.Mock()
		response.json.return_value = {'invalid_response': 'foo bar'}
		mock_requests_post.return_value = response

		mydata = mock.Mock()

		self.assertRaises(solrcl.SOLRResponseFormatError, self.solr.request, 'foo/bar', data=mydata)
		mock_requests_post.assert_called_with('http://localhost:8983/solr/foo/bar', params={'wt': 'json'}, headers={'Content-type': 'text/xml'}, data=mydata)


TEST_ADMIN_INFO_SYSTEM_OK = {'responseHeader': {'status': 0, 'QTime': 5}, 'lucene': {'solr-spec-version': '4.8.0', 'lucene-spec-version': '4.8.0'}}
TEST_ADMIN_INFO_SYSTEM_ERROR = {'responseHeader': {'status': 0, 'QTime': 5}, 'foo': 'bar'}
@mock.patch.object(solrcl.SOLRRequest, 'request')
class TestSOLRBase(unittest.TestCase):
	def test_init(self, mock_solrrequest):
		mock_solrrequest.return_value = TEST_ADMIN_INFO_SYSTEM_OK
		solr = solrcl.SOLRBase()
		self.assertEqual(solr.solr_version, '4.8.0')
		self.assertEqual(solr.lucene_version, '4.8.0')
		mock_solrrequest.assert_called_once_with('/solr/admin/info/system')

	def test_init_SOLRResponseFormatError(self, mock_solrrequest):
		mock_solrrequest.return_value = TEST_ADMIN_INFO_SYSTEM_ERROR
		self.assertRaises(solrcl.SOLRResponseFormatError, solrcl.SOLRBase)

	def test_init_NetworkError(self, mock_solrrequest):
		mock_solrrequest.side_effect = solrcl.SOLRNetworkError
		self.assertRaises(solrcl.SOLRNetworkError, solrcl.SOLRBase)

	def test_init_SOLRResponseError(self, mock_solrrequest):
		mock_solrrequest.side_effect = solrcl.SOLRResponseError("Error")
		self.assertRaises(solrcl.SOLRResponseError, solrcl.SOLRBase)


TEST_ADMIN_LUKE_OK = {
	'responseHeader': {'status': 0, 'QTime': 5},
	'schema': {
		'types': {
			'mytesttype': {'className': 'my.class.Type1'},
			'mytesttype2': {'className': 'my.class.Type2'}
		},
		'uniqueKeyField': 'myid',
		'fields': {
			'mytestfield': {'type': 'mytesttype', 'flags': 'ITS-------------', 'copySources': []},
			'mytestfieldmulti': {'type': 'mytesttype2', 'flags': 'I-S-M---OF-----l', 'copySources': []},
			'mytestfieldcopy': {'type': 'mytesttype', 'flags': 'IT--------------', 'copySources': ['mytestfield']}
		},
		'dynamicFields': {
			'mydynamicfield_*': {'type': 'mytesttype', 'flags': 'ITS-------------', 'copySources': []}
		}
	}
}
TEST_ADMIN_LUKE_OK_BLOCKJOIN = copy.deepcopy(TEST_ADMIN_LUKE_OK)
TEST_ADMIN_LUKE_OK_BLOCKJOIN['schema']['fields']['_root_'] = {'type': 'mytesttype', 'flags': 'ITS-------------', 'copySources': []}

TEST_ADMIN_LUKE_ERROR = {
	'responseHeader': {'status': 0, 'QTime': 5},
	'schema': { 'foo': 'bar'}
}

TEST_CORE_ADMIN_SYSTEM_OK = {'responseHeader': {'status': 0, 'QTime': 5}, 'core': {'directory': {'instance': 'instance_dir', 'data': 'data_dir'}}}
TEST_CORE_ADMIN_SYSTEM_ERROR = {'responseHeader': {'status': 0, 'QTime': 5}, 'core': {'foo': 'bar'}}

TEST_SELECT_BLOCKJOIN = {'responseHeader': {'status': 0, 'QTime': 5}, "response": {"numFound": 1234, "start": 0, "docs":[]}}


@mock.patch('solrcl.SOLRField')
@mock.patch('solrcl.SOLRType')
#There's no clean way to mock the SOLRBase class, so I mock up the SOLRRequest (re-testing SOLRBase.__init__ :( )
@mock.patch.object(solrcl.SOLRRequest, 'request')
class TestSOLRCore(unittest.TestCase):
	def _mock_SOLRRequest(self, sr, admin_luke, core_admin_system):
		#The instance of the class
		def mocked_request(resource, parameters={}, data=None, dataMIMEType='text/xml'):
			if resource == 'testcore/admin/luke' and parameters == {'show': 'schema'}:
				return admin_luke
			elif resource == '/solr/admin/info/system':
				return TEST_ADMIN_INFO_SYSTEM_OK
			elif resource == 'testcore/admin/system':
				return core_admin_system
			elif resource == 'testcore/select' and parameters == {"q": "_is_parent:true", "rows": 0}:
				return TEST_SELECT_BLOCKJOIN
			elif resource == 'testcore/select' and parameters == {"q": "nonexistingfield:somevalue", "rows": 0}:
				raise solrcl.SOLRResponseError("bla bla", httpStatus=httplib.BAD_REQUEST)

		sr.side_effect = mocked_request

	def _mock_SOLRType(self, st):
		def mock_solrtype_init(name, className):
			sti = mock.Mock()
			sti.name = name
			sti.className = className
			return sti

		st.side_effect = mock_solrtype_init

	def _mock_SOLRField(self, sf):
		def mock_solrfield_init(name, type, multi=False, copySources=None):
			sfi = mock.Mock()
			sfi.name = name
			sfi.type = type
			sfi.multi = multi
			if copySources is None:
				sfi.copySources = []
			else:
				sfi.copySources = copySources
			return sfi

		sf.side_effect = mock_solrfield_init

	def test_init(self, mock_solrrequest, mock_solrtype, mock_solrfield):
		self._mock_SOLRRequest(mock_solrrequest, TEST_ADMIN_LUKE_OK, TEST_CORE_ADMIN_SYSTEM_OK)
		self._mock_SOLRType(mock_solrtype)
		self._mock_SOLRField(mock_solrfield)
		solr = solrcl.SOLRCore('testcore')

		self.assertEqual(solr.types['mytesttype'].name, 'mytesttype')
		self.assertEqual(solr.types['mytesttype'].className, 'my.class.Type1')
		self.assertEqual(solr.types['mytesttype2'].name, 'mytesttype2')
		self.assertEqual(solr.types['mytesttype2'].className, 'my.class.Type2')

		self.assertEqual(solr.fields['mytestfield'].name, 'mytestfield')
		self.assertEqual(solr.fields['mytestfield'].type, solr.types['mytesttype'])
		self.assertEqual(solr.fields['mytestfield'].multi, False)
		self.assertEqual(solr.fields['mytestfield'].copySources, [])

		self.assertEqual(solr.fields['mytestfieldmulti'].name, 'mytestfieldmulti')
		self.assertEqual(solr.fields['mytestfieldmulti'].type, solr.types['mytesttype2'])
		self.assertEqual(solr.fields['mytestfieldmulti'].multi, True)
		self.assertEqual(solr.fields['mytestfieldmulti'].copySources, [])

		self.assertEqual(solr.fields['mytestfieldcopy'].name, 'mytestfieldcopy')
		self.assertEqual(solr.fields['mytestfieldcopy'].type, solr.types['mytesttype'])
		self.assertEqual(solr.fields['mytestfieldcopy'].multi, False)
		self.assertEqual(solr.fields['mytestfieldcopy'].copySources, [solr.fields['mytestfield']])

		self.assertEqual(solr.dynamicFields['mydynamicfield_*'].name, 'mydynamicfield_*')
		self.assertEqual(solr.dynamicFields['mydynamicfield_*'].type, solr.types['mytesttype'])
		self.assertEqual(solr.dynamicFields['mydynamicfield_*'].multi, False)
		self.assertEqual(solr.dynamicFields['mydynamicfield_*'].copySources, [])

	def test_init_with_blockjoin(self, mock_solrrequest, mock_solrtype, mock_solrfield):
		self._mock_SOLRRequest(mock_solrrequest, TEST_ADMIN_LUKE_OK_BLOCKJOIN, TEST_CORE_ADMIN_SYSTEM_OK)
		self._mock_SOLRType(mock_solrtype)
		self._mock_SOLRField(mock_solrfield)
		solr = solrcl.SOLRCore('testcore', blockjoin_condition="_is_parent:true")
		self.assertEqual(solr.fields['_root_'].name, '_root_')
		self.assertEqual(solr.fields['_root_'].type, solr.types['mytesttype'])
		self.assertEqual(solr.fields['_root_'].multi, False)
		self.assertEqual(solr.fields['_root_'].copySources, [])

	def test_init_with_blockjoin_noroot(self, mock_solrrequest, mock_solrtype, mock_solrfield):
		self._mock_SOLRRequest(mock_solrrequest, TEST_ADMIN_LUKE_OK, TEST_CORE_ADMIN_SYSTEM_OK)
		self._mock_SOLRType(mock_solrtype)
		self._mock_SOLRField(mock_solrfield)
		self.assertRaises(solrcl.MissingRequiredField, solrcl.SOLRCore, 'testcore', blockjoin_condition="_is_parent:true")

	def test_init_with_blockjoin_wrongcondition(self, mock_solrrequest, mock_solrtype, mock_solrfield):
		self._mock_SOLRRequest(mock_solrrequest, TEST_ADMIN_LUKE_OK_BLOCKJOIN, TEST_CORE_ADMIN_SYSTEM_OK)
		self._mock_SOLRType(mock_solrtype)
		self._mock_SOLRField(mock_solrfield)
		self.assertRaises(solrcl.SOLRResponseError, solrcl.SOLRCore, 'testcore', blockjoin_condition="nonexistingfield:somevalue")

	def test_init_wrong_solr_response_in_fields(self, mock_solrrequest, mock_solrtype, mock_solrfield):
		self._mock_SOLRRequest(mock_solrrequest, TEST_ADMIN_LUKE_ERROR, TEST_CORE_ADMIN_SYSTEM_OK)
		self.assertRaises(solrcl.SOLRResponseFormatError, solrcl.SOLRCore, 'testcore')

	def test_init_wrong_solr_response_in_core_admin(self, mock_solrrequest, mock_solrtype, mock_solrfield):
		self._mock_SOLRRequest(mock_solrrequest, TEST_ADMIN_LUKE_OK, TEST_CORE_ADMIN_SYSTEM_ERROR)
		self.assertRaises(solrcl.SOLRResponseFormatError, solrcl.SOLRCore, 'testcore')

	def test_init_nonexistent_core(self, mock_solrrequest, mock_solrtype, mock_solrfield):
		mock_solrrequest.side_effect = solrcl.SOLRResponseError("Not found", httplib.NOT_FOUND)
		self.assertRaises(solrcl.SOLRResponseError, solrcl.SOLRCore, 'nonexistingcore')


if __name__ == '__main__':
        #logger = logging.getLogger('solrcl')
        #loghandler = logging.StreamHandler()
        #logger.addHandler(loghandler)
        #logger.setLevel(logging.DEBUG)
        #loghandler.setLevel(logging.DEBUG)
        unittest.main()

