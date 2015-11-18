# -*- coding: utf8 -*-
"""Classes for SOLR core"""
import httplib
import datetime
import time
import multiprocessing
import multiprocessing.dummy
import warnings
import threading
import Queue
import logging
    
from solrcl.base import *
from solrcl.solrtype import *
from solrcl.solrfield import *
from solrcl.document import *
import solrcl.exceptions

#Create a custom logger
logger = logging.getLogger("solrcl")
logger.setLevel(logging.DEBUG)

SOLR_REPLICATION_DATETIME_FORMAT = '%a %b %d %H:%M:%S %Z %Y'

class MissingRequiredField(exceptions.SOLRError):
    """Exception raised when a required field is missing in the schema"""
    pass

class DocumentNotFound(exceptions.SOLRError):
    """Exception raised when a getDoc call by id returns no documents"""
    pass

class SOLRReplicationError(exceptions.SOLRError):
    """Exception raised for errors in replication calls"""
    pass

class SOLRCore(SOLRBase):
    """Class representing SOLR core with methods for acting on it"""
    def __init__(self, core, domain=DEFAULT_SOLR_DOMAIN, port=DEFAULT_SOLR_PORT, blockjoin_condition=None):
        self.core = core
        super(SOLRCore, self).__init__(domain=domain, port=port)
        self._setLogger()
        try:
            self._setSchema()
            self._setDirs()
        except SOLRResponseError as e:
            if e.httpStatus == httplib.NOT_FOUND:
                raise SOLRResponseError("Core \"{0}\" does not exist".format(core))
            else:
                raise e

        self.blockjoin_condition = blockjoin_condition
        #blockjoin cores need _root_ field
        if not self.blockjoin_condition is None:
            if not self.fields.has_key('_root_'):
                raise MissingRequiredField, "Missing requested field _root_ for blockjoin enabled cores"

            #Check if blockjoin condition is valid
            try:
                self.select({"q": self.blockjoin_condition, "rows": 0})
            except SOLRResponseError, e:
                raise SOLRResponseError("Invalid blockjoin condition: '%s': %s" % (self.blockjoin_condition, e), httpStatus=e.httpStatus)

        #Set caches
        self.cache = {}

        self.logger.debug("Core opened")

    def _setLogger(self):
        """Setup a custom logger for the class customized for printing solr context"""
        self.logger = logging.LoggerAdapter(logger, {'domain': self.domain, 'port': self.port, 'core': self.core})

    def _setTypes(self, solrtypesdict):
        """Setup core types from schema"""
        self.types = {}
        for (typename, typespecs) in solrtypesdict.iteritems():
            self.types[typename] = SOLRType(typename, typespecs['className'])

    def _setFields(self, solrfieldsdict, fieldsdict):
        """Setup core fields from schema"""
        for (fieldname, fieldspecs) in solrfieldsdict.iteritems():
            fieldsdict[fieldname] = SOLRField(
                fieldname,
                self.types[fieldspecs['type']],
                multi={'M': True}.get(fieldspecs['flags'][4], False)
                )

    def _setSchema(self):
        """Setup core informations from schema"""
        try:
            data = self.request("admin/luke", parameters={'show': "schema"})

            self._setTypes(data['schema']['types'])
            self.fields = {}
            self._setFields(data['schema']['fields'], self.fields)
            self.dynamicFields = {}
            self._setFields(data['schema']['dynamicFields'], self.dynamicFields)

            #Set copySources
            for (fieldname, fieldspecs) in data['schema']['fields'].iteritems():
                for copyfieldname in fieldspecs['copySources']:
                    self.fields[fieldname].copySources.append(self.fields[copyfieldname])

            #Could we have copyfield for dynamic fields? #FIXME

            self.id_field = data['schema']['uniqueKeyField']
        except KeyError, e:
            raise SOLRResponseFormatError, "Wrong response format: %s %s - %s" % (KeyError, e, data)

    def _setDirs(self):
        """Setup core directories"""
        try:
            data = self.request("admin/system")['core']
            self.instanceDir = data['directory']['instance']
            self.dataDir = data['directory']['data']
        except KeyError as e:
            #Key errors in this calls should occurr only when SOLR response is not as expected (though it is formally correct)
            raise SOLRResponseFormatError, "Wrong response format: %s %s - %s" % (KeyError, e, data)

    def request(self, resource, parameters={}, data=None, dataMIMEType=None):
	"""Wraps base request method adding corename to relative requests.
absolute requests are left as they are"""
        #Absolute path left "as is"
        if resource.startswith('/'):
            r = resource
        else:
            r = "{0}/{1}".format(self.core, resource)
        return super(SOLRCore, self).request(r, parameters=parameters, data=data, dataMIMEType=dataMIMEType)

    def ping(self):
        """admin/ping SOLR request"""
        return self.request('admin/ping', parameters={'ts': '{0}'.format(time.mktime(datetime.datetime.now().timetuple()))})

    def select(self, query):
        """select SOLR request, query should be a dictionary containing query parameters"""
        return self.request('select', parameters=query)

    def _iterResponseDocs(self, response, fields):
        """Transform SOLR json response for select in an iterator over tuples"""
        docs = response['response']['docs']
        for doc in docs:
            yield tuple(doc.get(f) for f in fields)

    def listFieldValuesIter(self, field, filter=None):
        """Iterates over all distinct values for a field"""
        if filter is None:
            query = '*:*'
        else:
            query = filter

        parameters = {
            'q': query,
            'facet': 'true',
            'facet.field': field,
            'facet.limit': '-1',
            'facet.mincount': '1',
            'rows': '0'
        }

        response = self.select(parameters)

        for r in zip(response['facet_counts']['facet_fields'][field][::2], response['facet_counts']['facet_fields'][field][1::2]):
            yield r

    def selectAllIter(self, query, fields=None, limit=None, blocksize=10000, parallel=6, start_blocksize=100, sort=''):
        """Iterates over all results for query as tuples of field values. query is a SOLR query string, fields are
the fields to be retrieved, limit is the maximum number of result to return, parallel is the number of simultaneous
requests to SOLR, blocksize is the number of documents to retrieve per request, start_blocksize is the number of 
documents to retrieve for the only first request (to optimize response times for smaller result sets), sort is the
sort parameter to pass to SOLR"""
        if sort and parallel > 1:
            parallel = 1
            warnings.warn("Cannot sort parallel query: using single process")
        if fields is None:
            fields = (self.id_field,)
        self.logger.debug("Selecting fields ({0}) for records matching: '{1}'".format(','.join(fields), query))

        if not limit is None and blocksize > limit:
            blocksize = limit

        start = 0

        #First request with small blocksize to retrieve some docs and numFound. For small requests it is sufficient
        response = self.select({'q': query, 'fl': ','.join(fields), 'rows': start_blocksize, 'start': start, 'sort': sort})

        numFound = response['response']['numFound']
        start += start_blocksize
        selected = 0

        for d in self._iterResponseDocs(response, fields):
            if limit is None or selected < limit:
                yield d
                selected += 1
            else:
                break

        #The multiprocessing.dummy module exposes same functionalities as multiprocessing module, but using threads, not subprocesses.
        p = multiprocessing.dummy.Pool(parallel)
        queries = ({'q': query, 'fl': ','.join(fields), 'rows': blocksize, 'start': start, 'sort': sort} for start in xrange(start, numFound, blocksize))

        for response in p.imap_unordered(self.select, queries):
            for d in self._iterResponseDocs(response, fields):
                if limit is None or selected < limit:
                    yield d
                    selected += 1
                else:
                    break

        self.logger.debug("{0} records selected".format(selected))

    def listFieldsIter(self, fields=None, limit=None, blocksize=10000, parallel=6, filter=None, sort=''):
        """Iterates over all values for given fields"""
        if fields is None:
            fields = (self.id_field,)
        if filter is None:
            query = "*:*"
        else:
            query = filter

        msg = "Extracting fields ({0}) for all records".format(', '.join(fields))
        if not filter is None:
            msg += " matching \"{0}\"".format(query)
        self.logger.debug(msg)

        return self.selectAllIter(query, fields, limit=limit, blocksize=blocksize, parallel=parallel, sort=sort)

    def listParentFieldsIter(self, fields=None, limit=None, blocksize=10000, parallel=6, filter=None, sort=''):
        if not filter is None:
            filter = "(%s) AND (%s)" % (self.blockjoin_condition, filter)
        else:
            filter = self.blockjoin_condition
        return self.listFieldsIter(fields=fields, limit=limit, blocksize=blocksize, parallel=parallel, filter=filter, sort=sort)

    def listFieldsDict(self, fields=None, limit=None, filter=None):
        res = {}
        if fields is None:
            fields = ()
        fields = (self.id_field,) + fields
        self.logger.debug("Saving fields ({0}) in dict for all records".format(', '.join(fields)))
        for values in self.listFieldsIter(fields=fields, limit=limit, filter=filter):
            if len(values) > 1:
                res[values[0]] = values[1:]
            else:
                res[values[0]] = True

        self.logger.debug("{0} records saved in dict".format(len(res)))
        return res

    def listNullFieldIter(self, nullfield, fields=None, limit=None):
        if fields is None:
            fields = (self.id_field,)
        return self.selectAllIter("-{0}:[* TO *]".format(nullfield), fields, limit=limit)

    def numRecord(self):
        return self.select({'q': '*:*', 'rows': 0})['response']['numFound']

    def update(self, parameters={}, data=None, dataMIMEType='text/xml'):
        return self.request('update', parameters=parameters, data=data, dataMIMEType=dataMIMEType)

    def extractHTML(self, data, dataMIMEType):
        return self.request('update/extract', parameters={'extractOnly': 'true', 'extractFormat': 'html'}, data=data, dataMIMEType=dataMIMEType)['']

    def commit(self):
        out = self.update(data="<commit/>", dataMIMEType='text/xml')
        self.logger.info("Commit executed in {0} ms".format(out['responseHeader']['QTime']))
        return out

    def optimize(self):
        out = self.update(data="<optimize/>", dataMIMEType='text/xml')
        self.logger.info("Optimize executed in {0} ms".format(out['responseHeader']['QTime']))
        return out

    def deleteByQueries(self, queries):
        def gen():
            yield '<delete>'
            for q in queries:
                yield "<query>%s</query>" % q
            yield '</delete>'

        return self.update(data=gen(), dataMIMEType='text/xml')

    def deleteByQuery(self, query):
        return self.deleteByQueries((query,))

    def deleteByIds(self, ids):
        return self.deleteByQueries("{0}:{1}".format(self.id_field, solrid) for solrid in ids)

    def deleteByParentIds(self, ids):
        """Remove documents whose ids are in ids iterator including their children, if any"""
        if self.blockjoin_condition:
            return self.deleteByQueries("_root_:{0} OR {1}:{0}".format(solrid, self.id_field) for solrid in ids)
        else:
            return self.deleteByIds(ids)

    def dropIndex(self):
        out = self.deleteByQuery("*:*")
        self.logger.debug(repr(out))
        self.logger.info("All records deleted in {0} ms".format(out['responseHeader']['QTime']))
        self.commit()

    def listBlockJoinParentIdsIter(self, solrid=None):
        """Iterates over parents ids that are in block join (and then need to be updated together with their child documents)"""
        #This query extract documents that are parents and have children (_root:[* TO *])
        if self.blockjoin_condition:
            q = "(%s) AND _root_:[* TO *]" % self.blockjoin_condition
        else:
            return (_ for _ in ())

        if not solrid is None:
            q += ' AND %s:"%s"' % (self.id_field, solrid)

        return (r[0] for r in self.selectAllIter(q))

    def listBlockJoinChildIdsIter(self, solrid=None):
        """Iterates over children ids that are in block join (and then need to be updated together with their child documents)"""
        #This query extract documents that are parents and have children (_root:[* TO *])
        if self.blockjoin_condition:
            q = "-(%s) AND _root_:[* TO *]" % self.blockjoin_condition
        else:
            return (_ for _ in ())

        if not solrid is None:
            q += ' AND %s:"%s"' % (self.id_field, solrid)

        return (r[0] for r in self.selectAllIter(q))

    def _isInIterDoc(self, solrid, iterator_f, prefetch=False):
        """If id is in iterator returns True, else False. iterator should accept a solrid optional parameter. If prefetch i true result is cached and prefetched"""
        cache_name = "_" + iterator_f.__name__ + "_ids_cache"
        if prefetch and not self.cache.has_key(cache_name):
            self.logger.info("Prefetching cache for %s" % iterator_f.__name__)
            self.cache[cache_name] = set()
            cache = self.cache[cache_name]
            for temp_solrid in iterator_f():
                cache.add(temp_solrid)
        if prefetch:
            return solrid in self.cache[cache_name]
        else:
            g = iterator_f(solrid=solrid)
            try:
                g.next()
                #When there is at least 1 document matching it is blockjoin
                return True
            except StopIteration:
                #Otherwise it isn't
                return False

    def clearCache(self):
        self.cache = {}

    def isBlockJoinParentDoc(self, solrid, prefetch=False):
        return self._isInIterDoc(solrid, self.listBlockJoinParentIdsIter, prefetch=prefetch)

    def isBlockJoinChildDoc(self, solrid, prefetch=False):
        return self._isInIterDoc(solrid, self.listBlockJoinChildIdsIter, prefetch=prefetch)

    def getDoc(self, solrid, include_reserved_fields=(), get_child_docs=True):
        """Returns a SOLRDocument instance with data from core for id solrid. Internal SOLR fields (starting and ending with "_") are not returned unless listed in include_reserved_fields. If get_child_docs is True will have child docs retrieved from blockjoin"""
        res = self.select({"q": '%s:"%s"' % (self.id_field, solrid), "rows": "1"})
        if res['response']['numFound'] > 0:
            doc = SOLRDocument(u'changeme', self)
            for (fieldname, fieldvalue) in res['response']['docs'][0].iteritems():
                if fieldname.startswith('_') and fieldname.endswith('_') and not fieldname in include_reserved_fields:
                    pass
                else:
                    if isinstance(fieldvalue, list):
                        doc.setField(fieldname, [self.fields[fieldname].type.deserialize(x) for x in fieldvalue])
                    else:
                        doc.setField(fieldname, self.fields[fieldname].type.deserialize(fieldvalue))

            if self.blockjoin_condition and get_child_docs:
                for (child_solrid,) in self.selectAllIter('{!child of="%s"}%s:"%s"' % (self.blockjoin_condition, self.id_field, solrid)):
                    child_doc = self.getDoc(child_solrid, include_reserved_fields=include_reserved_fields, get_child_docs=False)
                    doc.addChild(child_doc)

            return doc

        else:
            raise DocumentNotFound, 'Document "%s" not found' % solrid

    def _loadXMLDocs(self, docs):
        def gen():
            yield '<add>'
            for doc in docs:
                yield doc
                self.logger.debug("DATA: %s" % doc)
            yield '</add>'

        return self.update(data=gen(), dataMIMEType="text/xml; charset=utf-8")

    def _loadXMLDocsFromQueue(self, q, stop):
	def gen():
                #After 5 seconds of inactivity sends however a \n
                #to keep the connection connection alive.
                #Each object in the query is a string <doc>...</doc>
                #therefore there is no data corruption (\n are ignored)
                #stop is an Event signal
		while not stop.is_set():
			try:
				xmldoc = q.get(True, 5)
				yield xmldoc
                        	q.task_done()
			except Queue.Empty:
				yield "\n"

	return self._loadXMLDocs(gen())

    def loadEmptyDocs(self, ids):
        """Loads empty docs with id from ids iterator"""
        return self._loadXMLDocs('<doc><field name="{0}" null="false">{1}</field></doc>'.format(self.id_field, solr_id) for solr_id in ids)

    def loadDocs(self, docs, merge_child_docs=False, parallel=1):
        """Load documents from docs iterator. docs should iterate over SOLRDocument instances. This function transparently manages blockjoin updates. merge_child_docs=False replace child docs in core with child_docs in docs. merge_child_docs=True update child documents also, based in id field"""
        def gen():
            for newdoc in docs:
                if newdoc.hasChildDocs() or self.isBlockJoinParentDoc(newdoc.id, prefetch=True):
                    #Merge provided doc with solr doc to simulate update
                    newversion = newdoc.getFieldDefault('_version_', 0)
                    try:
                        #exclude_reserved_fields=False because I need to check _version_ field
                        currentdoc = self.getDoc(newdoc.id, include_reserved_fields=('_version_',))
                        currentversion = currentdoc.getFieldDefault('_version_', 0)

                        if newversion < 0:
                            #can't update: When version < 0 document must not already exists in core
                            warnings.warn("Can't update document %s: version < 0 and document exists in core" % newdoc.id, SOLRDocumentWarning)
                            continue

                        elif newversion > 1 and newversion != currentversion:
                            #Can't update: when version > 1 must match with version in core
                            warnings.warn("Can't update document %s: version doesn't match (%s - %s)" % (newdoc.id, newversion, currentversion), SOLRDocumentWarning)
                            continue
                        else:
                            #All other cases are OK
                            currentdoc.update(newdoc, merge_child_docs=merge_child_docs)
                            doc2load = currentdoc
                            #When loading blockjoin documents we must delete documents before update
                            self.deleteByParentIds((newdoc.id,))
                    except DocumentNotFound:
                        if newversion > 0:
                            #Can't update: when version > 0 document must exists in core
                            warnings.warn("Can't update document %s: version > 0 and document does not exists in core" % (newdoc.id,), SOLRDocumentWarning)
                            continue
                        else:
                            #All other cases are OK
                            doc2load = newdoc
                    #Remove _version_: must not exists when loading new documents. Blockjoins are always new documents because we've already deleted the original version.
                    doc2load.removeField('_version_')
                    #Can't use update for blockjoin documents: SOLR doesn't load and raises no error.
                    yield doc2load.toXML(update=False)

                elif self.isBlockJoinChildDoc(newdoc.id, prefetch=True):
                    #Not yet supported: skip
                    warnings.warn("Can't update document %s: it is a child blockjoin doc. This use case is not yet supported" % (newdoc.id,), SOLRDocumentWarning)
                    continue
                else:
                    doc2load = newdoc
                    yield doc2load.toXML()

	#A FIFO Queue
        q = Queue.Queue()

	#A signal Event for stopping running threads
	stop = threading.Event()

	#Starts threads
	threads = []
        for _ in range(0,parallel):
		t = threading.Thread(target=self._loadXMLDocsFromQueue, args=(q, stop))
                t.start()
		threads.append(t)
		self.logger.debug("Starting thread %s" % t.name)
	try:
		#Fill the queue
		for d in gen():
			q.put(d)
			self.logger.debug("Put document in queue %s" % repr(q))
			self.logger.debug("%s" % d)

	finally:
		self.logger.debug("Joining queue")
		q.join()

		#Sending stop signal to threads
		stop.set()
		self.logger.debug("Queue joined")
		for t in threads:
			self.logger.debug("Joining thread %s", t.name)
			t.join()

        # invalidaate cache because documents have changed
        self.clearCache()

    def replicationCommand(self, command, **pars):
        pars['command'] = command
        return self.request('replication', parameters=pars)

    def startReplication(self, masterUrl=None):
        pars = {}
        if masterUrl:
            pars['masterUrl'] = masterUrl

        self.replication_start_timestamp = datetime.datetime.strptime(self.replicationCommand('details')['details']['slave']['currentDate'], SOLR_REPLICATION_DATETIME_FORMAT)
        #Wait at least 1 second to distinguish replication_start_timestamp and last_replication_timestamp if it is executed too rapidly (timestamp has 1 second resolution)
        time.sleep(1)

        out = self.replicationCommand('fetchindex', **pars)

        self.logger.info("Start replication command executed (replicate from {0})".format(masterUrl))

        self.replication_masterUrl = masterUrl

    def checkLastReplicationStatus(self, timeout_seconds=3600):
        is_running = True
        start_time = datetime.datetime.now()
        errors = 0
        while is_running:
            try:
                response = self.replicationCommand('details')
            except SOLRNetworkError, err:
                if errors < 10:
                    self.logger.warning("Error: %s" % err)
                    errors += 1
                    continue
                else:
                    raise

            is_running = {'true': True}.get(response['details']['slave']['isReplicating'], False)
            if not is_running:
                break
            elapsed_seconds = (datetime.datetime.now() - start_time).seconds
            if elapsed_seconds >= timeout_seconds:
                raise SOLRReplicationError, "Replication started but not ended in {0} seconds".format(timeout_seconds)
            self.logger.debug("Replication is running")
            time.sleep(5)

        last_replication = datetime.datetime.strptime(response['details']['slave'].get('indexReplicatedAt', 'Fri Jan 01 00:00:00 CEST 1960'), SOLR_REPLICATION_DATETIME_FORMAT)
        last_replication_failed = datetime.datetime.strptime(response['details']['slave'].get('replicationFailedAt', 'Fri Jan 01 00:00:00 CEST 1960'), SOLR_REPLICATION_DATETIME_FORMAT)
        if last_replication == last_replication_failed:
            #Last replication date is also last replication failed date, therefore last replication failed
            self.logger.error("Replication has failed")
            return (last_replication, False)
        else:
            return (last_replication, True)

    def waitReplication(self, poll_interval_seconds=10, max_attempts=10):
        if hasattr(self, 'replication_start_timestamp') and self.replication_start_timestamp != None:
            start_server_timestamp = self.replication_start_timestamp
        else:
            raise SOLRReplicationError, "No replication started: can't wait"

        self.logger.debug("Server timestamp = {0}".format(start_server_timestamp))
        last_replication_timestamp = start_server_timestamp
        attempts = 0
        while True:
            (last_replication_timestamp, status) = self.checkLastReplicationStatus()
            self.logger.debug("Last replication timestamp = {0}".format(last_replication_timestamp))
            attempts += 1
            if last_replication_timestamp <= start_server_timestamp:
                self.logger.warning("Replication not started in {0} seconds: retrying ({1} of {2})".format(poll_interval_seconds, attempts, max_attempts))
                if attempts >= max_attempts:
                    raise SOLRReplicationError, "Replication not started on slave after {0} attempts".format(max_attempts)
            else:
                break

            #If replication didn't start and we didn't reach max_attempts retry
            self.startReplication(masterUrl=self.replication_masterUrl)
            time.sleep(poll_interval_seconds)


        if status == False:
            raise SOLRReplicationError, "Replication failed: see log on slave server"

        end_server_timestamp = datetime.datetime.strptime(self.replicationCommand('details')['details']['slave']['currentDate'], SOLR_REPLICATION_DATETIME_FORMAT)
        self.logger.info("Replication executed in {0} s".format((end_server_timestamp - start_server_timestamp).seconds))
        return True

    def startAndWaitReplication(self, poll_interval_seconds=10, masterUrl=None, max_attempts=10):
        self.startReplication(masterUrl=masterUrl)
        self.waitReplication(poll_interval_seconds=poll_interval_seconds, max_attempts=max_attempts)

    def getIndexVersion(self):
        replication_details = self.replicationCommand('details')['details']
        replication_version = replication_details['indexVersion']
        replication_generation = replication_details['generation']
        index_info = self.request("admin/luke", parameters={"show": "index", "numTerms": 0})['index']
        index_version = index_info['version']
        return (index_version, replication_version, replication_generation)
