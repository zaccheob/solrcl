#!/usr/local/bin/python
# -*- coding: utf8 -*-

import warnings
import datetime
import unittest
import solrcl

import logging

TEST_SOLRCONFIG = """<?xml version="1.0" encoding="UTF-8" ?>
<config>
  <luceneMatchVersion>LUCENE_41</luceneMatchVersion>
  <lib dir="${solr.install.dir}/dist" regex="solr-cell-.*\.jar"/>
  <lib dir="${solr.install.dir}/contrib/extraction/lib" regex=".*\.jar"/>
  <directoryFactory name="DirectoryFactory" class="${solr.directoryFactory:solr.NRTCachingDirectoryFactory}"/>
  <updateHandler class="solr.DirectUpdateHandler2"><updateLog/></updateHandler>
  <requestHandler name="/select" class="solr.SearchHandler">
     <lst name="defaults">
       <str name="echoParams">explicit</str>
       <int name="rows">10</int>
       <str name="df">id</str>
     </lst>
  </requestHandler>
  <requestHandler name="/update" class="solr.UpdateRequestHandler"  />
  <requestHandler name="/admin/" class="org.apache.solr.handler.admin.AdminHandlers" />
  <requestHandler name="/admin/ping" class="solr.PingRequestHandler">
    <lst name="invariants">
      <str name="q">solrpingquery</str>
    </lst>
    <lst name="defaults">
      <str name="echoParams">all</str>
    </lst>
  </requestHandler>
  <requestHandler name="/replication" class="solr.ReplicationHandler" startup="lazy">
    <lst name="master">
      <str name="enable">true</str>
    </lst>
  </requestHandler>
  <requestHandler name="/update/extract" class="solr.extraction.ExtractingRequestHandler" >
    <lst name="defaults">
      <str name="fmap.content">document_text</str>
      <str name="lowernames">true</str>
      <str name="uprefix">ignored_</str>
      <str name="captureAttr">false</str>
    </lst>
  </requestHandler>
</config>"""

TEST_SCHEMA = """<?xml version="1.0" ?>
<schema version="1.5">
  <types>
    <fieldtype name="string"  class="solr.StrField" sortMissingLast="true" omitNorms="true"/>
    <fieldType name="long" class="solr.TrieLongField" precisionStep="0" positionIncrementGap="0"/>
    <fieldType name="int" class="solr.TrieIntField" precisionStep="0" positionIncrementGap="0"/>
    <fieldType name="date" class="solr.TrieDateField" precisionStep="0" positionIncrementGap="0"/>
    <fieldType name="bool" class="solr.BoolField"/>
  </types>
  <fields>
    <field name="_version_" type="long" indexed="true"  stored="true"/>
    <field name="_root_" type="string" indexed="true"  stored="true"/>
    <field name="_is_parent" type="bool" indexed="true"  stored="true"/>
    <field name="id" type="string" indexed="true" stored="true" multiValued="false" />
    <field name="testdate" type="date" indexed="true" stored="true" multiValued="false" />
    <field name="testint" type="int" indexed="true" stored="true" multiValued="false" />
    <field name="testmulti" type="string" indexed="true" stored="true" multiValued="true" />
  </fields>
  <uniqueKey>id</uniqueKey>
</schema>"""


def initTestCore():
	try:
		solrcl.SOLRCore('test_solrcl')
	except:
		solrcl.initCore('test_solrcl', '/tmp/test_solrcl', TEST_SOLRCONFIG, TEST_SCHEMA)


class TestSolrlibBase(unittest.TestCase):
	def setUp(self):
		initTestCore()

	def tearDown(self):
		solrcl.freeCore('test_solrcl')


class TestSolrlibReadConfig(TestSolrlibBase):
	def setUp(self):
		super(TestSolrlibReadConfig, self).setUp()
		self.solr = solrcl.SOLRCore('test_solrcl')
	def test_init_nonexistent_core(self):
		self.assertRaises(solrcl.SOLRResponseError, solrcl.SOLRCore, ('this_core_does_not_exist',))
	def test_ping(self):
		self.solr.ping()
	def test_info(self):
		self.solr.solr_version
		self.solr.lucene_version
	def test_schema(self):
		self.assertEqual(self.solr.types['string'].name, 'string')
		self.assertEqual(self.solr.types['string'].className, 'org.apache.solr.schema.StrField')
		self.assertEqual(self.solr.types['long'].name, 'long')
		self.assertEqual(self.solr.types['long'].className, 'org.apache.solr.schema.TrieLongField')
		self.assertEqual(self.solr.types['int'].name, 'int')
		self.assertEqual(self.solr.types['int'].className, 'org.apache.solr.schema.TrieIntField')
		self.assertEqual(self.solr.types['date'].name, 'date')
		self.assertEqual(self.solr.types['date'].className, 'org.apache.solr.schema.TrieDateField')
		self.assertEqual(self.solr.fields['_version_'].name, '_version_')
		self.assertEqual(self.solr.fields['_version_'].type.name, 'long')
		self.assertEqual(self.solr.fields['_version_'].multi, False)
		self.assertEqual(self.solr.fields['id'].name, 'id')
		self.assertEqual(self.solr.fields['id'].type.name, 'string')
		self.assertEqual(self.solr.fields['id'].multi, False)
		self.assertEqual(self.solr.fields['testdate'].name, 'testdate')
		self.assertEqual(self.solr.fields['testdate'].type.name, 'date')
		self.assertEqual(self.solr.fields['testdate'].multi, False)
		self.assertEqual(self.solr.fields['testint'].name, 'testint')
		self.assertEqual(self.solr.fields['testint'].type.name, 'int')
		self.assertEqual(self.solr.fields['testint'].multi, False)
		self.assertEqual(self.solr.fields['testmulti'].name, 'testmulti')
		self.assertEqual(self.solr.fields['testmulti'].type.name, 'string')
		self.assertEqual(self.solr.fields['testmulti'].multi, True)
		self.assertEqual(self.solr.id_field, 'id')


class TestSolrlibWithDocs(TestSolrlibBase):
	def setUp(self):
		super(TestSolrlibWithDocs, self).setUp()
		self.solr = solrcl.SOLRCore('test_solrcl')
		#Load some documents
		docs = (
			'<doc><field name="id">A</field><field name="testdate">2014-01-31T17:20:00Z</field><field name="testint">1</field><field name="testmulti">A1</field><field name="testmulti">A2</field></doc>',
			'<doc><field name="id">B</field><field name="testdate">2014-01-31T17:21:00Z</field><field name="testint">1</field><field name="testmulti">B1</field></doc>',
			'<doc><field name="id">C</field><field name="testdate">2014-01-31T17:22:00Z</field><field name="testint">1</field><field name="testmulti">C1</field></doc>'
		)
		self.solr._loadXMLDocs(docs)

		docs = ('<doc><field name="id">%s</field><field name="testint">2</field></doc>' % x for x in range(0,100000))
		self.solr._loadXMLDocs(docs)

		self.solr.commit()
	def tearDown(self):
		self.solr.dropIndex()
		super(TestSolrlibWithDocs, self).tearDown()


class TestSolrlibSelect(TestSolrlibWithDocs):
	def test_numRecord(self):
		count = self.solr.numRecord()
		self.assertEqual(count, 100003)

	def test_select(self):
		res = self.solr.select({'q': 'testint:1', 'fl': 'id, testint'})
		self.assertEqual(res['response']['docs'], [{u'testint': 1, u'id': u'A'}, {u'testint': 1, u'id': u'B'}, {u'testint': 1, u'id': u'C'}])

	def test_select_wrongquery(self):
		self.assertRaises(solrcl.SOLRResponseError, self.solr.select, {'q': 'thisfielddoesnotexist:x'})

	def test_selectAllIter(self):
		expected_out = dict([((unicode(x),), True) for x in range(0,100000)])
		for out in self.solr.selectAllIter('testint:2', ('id',)):
			del(expected_out[out])

		self.assertEqual(expected_out, {})

	def test_selectAllIter_sorted(self):
		expected_out = [(unicode(x),) for x in range(0,100000)]
		expected_out.sort()
		real_out = list(self.solr.selectAllIter('testint:2', ('id',), sort="id ASC"))
		self.assertEqual(expected_out, real_out)
		
	def test_listFieldValuesIter(self):
		expected_out = [("1", 3), ("2", 100000)]
		out = []
		for r in self.solr.listFieldValuesIter('testint'):
			out.append(r)
		self.assertEqual(out, expected_out)

	def _get_expected_all_ids(self):
		expected_out = dict([(unicode(x), True) for x in range(0,100000)])
		expected_out['A'] = True
		expected_out['B'] = True
		expected_out['C'] = True
		return expected_out

	def test_listFieldsIter(self):
		expected_out = self._get_expected_all_ids()
		real_out = {}
		for (out,) in self.solr.listFieldsIter():
			if not real_out.has_key(out):
				real_out[out] = True
			else:
				raise ValueError("value %s extracted twice" % out)

		self.assertEqual(real_out, expected_out)

	def test_listFieldsDict(self):
		expected_out = self._get_expected_all_ids()
		out = self.solr.listFieldsDict()
		self.assertEqual(out, expected_out)

	def test_listNullFieldIter(self):
		expected_out = dict([(unicode(x), True) for x in range(0,100000)])
		real_out = {}
		for (out,) in self.solr.listNullFieldIter('testdate'):
			if not real_out.has_key(out):
				real_out[out] = True
			else:
				raise ValueError("value %s extracted twice" % out)

		self.assertEqual(real_out, expected_out)


class TestSolrlibDrop(TestSolrlibWithDocs):
	def test_dropIndex(self):
		self.solr.dropIndex()
		count = self.solr.numRecord()
		self.assertEqual(count, 0)


class TestSolrlibUpdates(TestSolrlibWithDocs):
	def test_commit(self):
		self.solr.commit()

	def test_optimize(self):
		self.solr.optimize()

	def test_deleteByQueries(self):
		queries = ["testmulti:A1", "testmulti:C1"]
		count_before = len([_ for _ in self.solr.selectAllIter(" OR ".join(queries))])
		self.solr.deleteByQueries(queries)
		self.solr.commit()
		count_after = len([_ for _ in self.solr.selectAllIter(" OR ".join(queries))])
		self.assertTrue(count_after < count_before)

	def test_deleteByQuery(self):
		query = "testmulti:B1"
		count_before = len([_ for _ in self.solr.selectAllIter(query)])
		self.solr.deleteByQuery(query)
		self.solr.commit()
		count_after = len([_ for _ in self.solr.selectAllIter(query)])
		self.assertTrue(count_after < count_before)

	def test_deleteByIds(self):
		ids = [1,2,3,4]
		count_before = len([_ for _ in self.solr.selectAllIter(" OR ".join(["id:%s" % i for i in ids]))])
		self.solr.deleteByIds(ids)
		self.solr.commit()
		count_after = len([_ for _ in self.solr.selectAllIter(" OR ".join(["id:%s" % i for i in ids]))])
		self.assertTrue(count_after < count_before)


class TestSolrlibReplication(TestSolrlibWithDocs):
	def setUp(self):
		super(TestSolrlibReplication, self).setUp()
		try:
			solrcl.SOLRCore('test_solrcl_repl')
		except:
			solrcl.initSlaveSolrCore('test_solrcl_repl', '/tmp/test_solrcl_repl', 'test_solrcl')

		self.solr_repl = solrcl.SOLRCore('test_solrcl_repl')

	def tearDown(self):
		solrcl.freeCore('test_solrcl_repl')
		super(TestSolrlibReplication, self).tearDown()

	def test_startAndWaitReplication(self):
		self.solr_repl.startAndWaitReplication(masterUrl='http://localhost:8983/solr/test_solrcl')

	def test_getIndexVersion(self):
		(iv, rv, rg) = self.solr_repl.getIndexVersion()


class TestSolrlibWithDocsBlockjoin(TestSolrlibBase):
	def setUp(self):
		super(TestSolrlibWithDocsBlockjoin, self).setUp()
		self.solr = solrcl.SOLRCore('test_solrcl', blockjoin_condition="_is_parent:true")
		#Load some documents
		docs = (
			'<doc><field name="id">A</field><field name="testdate">2014-01-31T17:20:00Z</field><field name="testint">1</field><field name="testmulti">A1</field><field name="testmulti">A2</field><field name="_is_parent">true</field><doc><field name="id">A.1</field><field name="testmulti">blabla</field><field name="_is_parent">false</field></doc></doc>',
			'<doc><field name="id">B</field><field name="testdate">2014-01-31T17:20:00Z</field><field name="testint">1</field><field name="testmulti">B1</field><field name="testmulti">B2</field><field name="_is_parent">true</field><doc><field name="id">B.1</field><field name="testmulti">blabla</field><field name="_is_parent">false</field></doc></doc>',
			'<doc><field name="id">C</field><field name="testdate">2014-01-31T17:20:00Z</field><field name="testint">1</field><field name="testmulti">B1</field><field name="testmulti">B2</field><field name="_is_parent">true</field></doc>',
		)
		self.solr._loadXMLDocs(docs)
		self.solr.commit()

	def tearDown(self):
		self.solr.dropIndex()
		super(TestSolrlibWithDocsBlockjoin, self).tearDown()

	def test_isBlockJoinParentDoc(self):
		self.assertTrue(self.solr.isBlockJoinParentDoc('A'))
		self.assertFalse(self.solr.isBlockJoinParentDoc('C'))
		self.assertFalse(self.solr.isBlockJoinParentDoc('A.1'))

		self.assertTrue(self.solr.isBlockJoinParentDoc('A', prefetch=True))
		self.assertFalse(self.solr.isBlockJoinParentDoc('C', prefetch=True))
		self.assertFalse(self.solr.isBlockJoinParentDoc('A.1', prefetch=True))

	def test_isBlockJoinChildDoc(self):
		self.assertFalse(self.solr.isBlockJoinChildDoc('A'))
		self.assertTrue(self.solr.isBlockJoinChildDoc('A.1'))

		self.assertFalse(self.solr.isBlockJoinChildDoc('A', prefetch=True))
		self.assertTrue(self.solr.isBlockJoinChildDoc('A.1', prefetch=True))

	def test_getDoc(self):
		#blockjoin document
		checkdoc = solrcl.SOLRDocument(u"A", self.solr)
		checkdoc.setField("testdate", datetime.datetime(2014, 1, 31, 17, 20, 0))
		checkdoc.setField("testint", 1)
		checkdoc.setField("testmulti", [u"A1", u"A2"])
		checkdoc.setField("_is_parent", True)
		childdoc = solrcl.SOLRDocument(u"A.1", self.solr)
		childdoc.setField("testmulti", u"blabla")
		childdoc.setField("_is_parent", False)
		checkdoc.addChild(childdoc)
		self.assertEqual(checkdoc, self.solr.getDoc("A"))

		#non blockjoin document
		checkdoc = solrcl.SOLRDocument(u"C", self.solr)
		checkdoc.setField("testdate", datetime.datetime(2014, 1, 31, 17, 20, 0))
		checkdoc.setField("testint", 1)
		checkdoc.setField("testmulti", [u"B1", u"B2"])
		checkdoc.setField("_is_parent", True)
		self.assertEqual(checkdoc, self.solr.getDoc("C"))

		#get internal fields also
		checkdoc = solrcl.SOLRDocument(u"A", self.solr)
		checkdoc.setField("testdate", datetime.datetime(2014, 1, 31, 17, 20, 0))
		checkdoc.setField("testint", 1)
		checkdoc.setField("testmulti", [u"A1", u"A2"])
		checkdoc.setField("_is_parent", True)
		for (version,) in self.solr.selectAllIter("id:A", fields=('_version_',)):
			pass
		checkdoc.setField("_version_", version)
		for (root,) in self.solr.selectAllIter("id:A", fields=('_root_',)):
			pass
		checkdoc.setField("_root_", root)
		
		childdoc = solrcl.SOLRDocument(u"A.1", self.solr)
		childdoc.setField("testmulti", u"blabla")
		childdoc.setField("_is_parent", False)
		for (root,) in self.solr.selectAllIter("id:A.1", fields=('_root_',)):
			pass
		childdoc.setField("_root_", root)
		checkdoc.addChild(childdoc)

		self.assertEqual(checkdoc, self.solr.getDoc("A", include_reserved_fields=('_version_', '_root_')))

		self.assertRaises(solrcl.DocumentNotFound, self.solr.getDoc, "nonexistingid")


class TestSolrlibLoadDocs(TestSolrlibBase):
	def setUp(self):
		super(TestSolrlibLoadDocs, self).setUp()
		self.solr = solrcl.SOLRCore('test_solrcl', blockjoin_condition="_is_parent:true")
		#Create a few SOLRDocuments to use in test cases
		doc = solrcl.SOLRDocument(u"A", self.solr)
		doc.setField("testdate", datetime.datetime(2015, 6, 11, 10, 00, 0))
		doc.setField("testint", 1)
		doc.setField("testmulti", [u"A1", u"A2"])
		doc.setField("_is_parent", True)
		self.DOC_NOBLOCKJOIN = doc


		doc = solrcl.SOLRDocument(u"B", self.solr)
		doc.setField("testdate", datetime.datetime(2015, 6, 11, 10, 00, 0))
		doc.setField("testint", 2)
		doc.setField("testmulti", [u"B1", u"B2"])
		doc.setField("_is_parent", True)
		childdoc = solrcl.SOLRDocument(u"B.1", self.solr)
		childdoc.setField("testmulti", u"B.1.1")
		doc.addChild(childdoc)
		self.DOC_BLOCKJOIN = doc

	def test_loadDocs_base(self):
		self.solr.loadDocs((self.DOC_NOBLOCKJOIN, self.DOC_BLOCKJOIN))
		self.solr.commit()
		self.solr.clearCache()
		self.assertEqual(self.DOC_NOBLOCKJOIN, self.solr.getDoc("A"))
		self.assertEqual(self.DOC_BLOCKJOIN, self.solr.getDoc("B"))

	def test_update_noblockjoin(self):
		self.solr.loadDocs((self.DOC_NOBLOCKJOIN,))
		self.solr.commit()
		self.solr.clearCache()

		docupdate = solrcl.SOLRDocument(u"A", self.solr)
		docupdate.setField("testint", 2)

		self.solr.loadDocs((docupdate,))
		self.solr.commit()

		checkdoc = self.DOC_NOBLOCKJOIN.clone()
		checkdoc.setField("testint", 2)

		self.assertEqual(checkdoc, self.solr.getDoc("A"))

	def test_update_blockjoin(self):
		self.solr.loadDocs((self.DOC_NOBLOCKJOIN, self.DOC_BLOCKJOIN))
		self.solr.commit()
		self.solr.clearCache()

		docupdate = solrcl.SOLRDocument(u"B", self.solr)
		docupdate.setField("testint", 4)

		self.solr.loadDocs((docupdate,))
		self.solr.commit()

		checkdoc = self.DOC_BLOCKJOIN.clone()
		checkdoc._child_docs = []
		checkdoc.setField("testint", 4)

		print self.solr.getDoc("B").toXML()
		print checkdoc.toXML()
		self.assertEqual(checkdoc, self.solr.getDoc("B"))

	def test_update_blockjoin_merge_child_docs(self):
		self.solr.loadDocs((self.DOC_NOBLOCKJOIN, self.DOC_BLOCKJOIN))
		self.solr.commit()
		self.solr.clearCache()

		docupdate = solrcl.SOLRDocument(u"B", self.solr)
		docupdate.setField("testint", 4)
		newchild = solrcl.SOLRDocument(u"B.2", self.solr)
		docupdate.addChild(newchild)
		updatedchild = solrcl.SOLRDocument(u"B.1", self.solr)
		updatedchild.setField("testmulti", u"updated")
		docupdate.addChild(updatedchild)

		self.solr.loadDocs((docupdate,))
		self.solr.commit()

		checkdoc = self.DOC_BLOCKJOIN.clone()
		checkdoc.setField("testint", 4)
		checkdoc.getChildDocs()[0].setField("testmulti", u"updated")
		checkdoc.addChild(newchild)

		self.assertEqual(checkdoc, self.solr.getDoc("B"))

	def test_update_optimistic_concurrence_different_version(self):
		self.solr.loadDocs((self.DOC_NOBLOCKJOIN, self.DOC_BLOCKJOIN))
		self.solr.commit()
		self.solr.clearCache()

		docupdate1 = self.solr.getDoc("B", include_reserved_fields='_version_')
		docupdate1.setField("testint", 4)

		docupdate2 = self.solr.getDoc("B", include_reserved_fields='_version_')
		docupdate2.setField("testint", 3)

		self.solr.loadDocs((docupdate1,))
		self.solr.commit()

		#This should raise a warning because version has changed
		with warnings.catch_warnings(record=True) as w:
			self.solr.loadDocs((docupdate2,))
			docupdate1.removeField('_version_')
			self.assertEqual(docupdate1, self.solr.getDoc("B"))
			self.assertEqual(len(w), 1)
			self.assertEqual(w[0].category, solrcl.SOLRDocumentWarning)

	def test_update_optimistic_concurrence_new_document(self):
		self.solr.loadDocs((self.DOC_NOBLOCKJOIN, self.DOC_BLOCKJOIN))
		self.solr.commit()
		self.solr.clearCache()

		docupdate = self.DOC_BLOCKJOIN.clone()
		#This is to force that the document should be loaded only if is new (and it is not true)
		docupdate.setField('_version_', -1)
		docupdate.setField('testint', 99)

		with warnings.catch_warnings(record=True) as w:
			self.solr.loadDocs((docupdate,))
			self.solr.commit()
			#Checks that document has not changed
			self.assertEqual(self.DOC_BLOCKJOIN, self.solr.getDoc("B"))
			#A warning is raised
			self.assertEqual(len(w), 1)
			self.assertEqual(w[0].category, solrcl.SOLRDocumentWarning)

	def test_update_optimistic_concurrence_existing_document(self):
		self.solr.loadDocs((self.DOC_NOBLOCKJOIN, self.DOC_BLOCKJOIN))
		self.solr.commit()
		self.solr.clearCache()

		docupdate = solrcl.SOLRDocument(u"C", self.solr)
		docupdate.setField('_version_', 1)
		childdoc = solrcl.SOLRDocument(u"C.1", self.solr)
		docupdate.addChild(childdoc)
		#If version > 0 document must exists
		with warnings.catch_warnings(record=True) as w:
			self.solr.loadDocs((docupdate,))
			self.solr.commit()
			#Checks that document has not been created
			self.assertRaises(solrcl.DocumentNotFound, self.solr.getDoc, "C")
			self.assertEqual(len(w), 1)
			self.assertEqual(w[0].category, solrcl.SOLRDocumentWarning)
		

if __name__ == '__main__':
	solrcl_logger = logging.getLogger("solrcl")
	handler=logging.StreamHandler()
	handler.setLevel(logging.INFO)
	solrcl_logger.addHandler(handler)
	unittest.main()
