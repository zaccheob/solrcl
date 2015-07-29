# -*- coding: utf8 -*-

import logging
import requests
import httplib

import exceptions

#Create a custom logger
logger = logging.getLogger("solrcl")
logger.setLevel(logging.DEBUG)

DEFAULT_SOLR_DOMAIN="localhost"
DEFAULT_SOLR_PORT=8983

class SOLRNetworkError(exceptions.SOLRError): pass
class SOLRResponseError(exceptions.SOLRError):
	def __init__(self, message, httpStatus=httplib.OK):
		Exception.__init__(self, message)
		self.httpStatus = httpStatus
class SOLRResponseFormatError(exceptions.SOLRError): pass


class SOLRRequest(object):
	"""
Base class providing SOLR connection and a method :attr:`.request` for making http requests to SOLR. This class should not be used directly.
	"""
	def __init__(self, domain=DEFAULT_SOLR_DOMAIN, port=DEFAULT_SOLR_PORT):
		self.domain = domain
		self.port = port
		self.logger = logging.LoggerAdapter(logger, {'domain': self.domain, 'port': self.port})

	def request(self, resource, parameters={}, data=None, dataMIMEType='text/xml'):

		#Infers request method from the value of 'data' parameter
		if not data is None:
			req_method = requests.post
			headers = {'Content-type': dataMIMEType}
		else:
			req_method = requests.get
			headers = {}

		#Set default output /Re/presentation for /S/tate /T/ransfer
		parameters.setdefault('wt', 'json')

		#if absolute, use "as is" otherwise use standard solr URI
		if not resource.startswith('/'):
			resource = '/solr/{0}'.format(resource)

		resource = 'http://{0}:{1}{2}'.format(self.domain, self.port, resource)

		self.logger.debug("Requesting: {0} {1}".format(resource, parameters))

		try:
			#This makes the request
			r = req_method(resource, params=parameters, headers=headers, data=data)

			#Only json supported for now...
			if r.headers.get('content-type') in ('application/json; charset=UTF-8', 'application/json'):
				try:
					response = r.json()
				except ValueError, err:
					raise SOLRResponseError, "Error in parsing json: {0}".format(err)
			else:
				#Not 200 HTTP code raise exception
				#Note that exception is raised only if response is not json. Error responses that return correct json are managed after
				r.raise_for_status()
				raise SOLRResponseError, "Unsupported response content type {0}".format(r.headers.get('content-type'))

		except requests.ConnectionError, err:
			raise SOLRNetworkError("{0}".format(err))
		except requests.RequestException, err:
			raise SOLRResponseError("HTTP request error: {0}".format(err), httpStatus=r.status_code)
			
		try:
			if response.has_key('responseHeader') and response['responseHeader']['status'] == 0:
				return response
			elif not response.has_key('responseHeader') and response.has_key('error'):
				raise SOLRResponseError, "Error in SOLR response: {0} {1} {2}".format(response['error']['code'], response['error']['msg'], response['error'].get('trace', ''))
			else:
				raise SOLRResponseError, "Error in SOLR response: {0} {1}".format(response['responseHeader']['status'], response['error']['msg'])
		except KeyError, msg:
			raise SOLRResponseFormatError, "Wrong response format: {0}: {1} - {2}".format(KeyError, msg, repr(response))


class SOLRBase(SOLRRequest):
	def __init__(self, domain=DEFAULT_SOLR_DOMAIN, port=DEFAULT_SOLR_PORT):
		super(SOLRBase, self).__init__(domain=domain, port=port)

		data = self.request("/solr/admin/info/system")
		try:
			self.solr_version = data['lucene']['solr-spec-version']	
			self.lucene_version = data['lucene']['lucene-spec-version']	
		except KeyError, err:
			raise SOLRResponseFormatError, "Wrong response format: {0} - {1}".format(err, repr(data))

		self.logger.debug("Connection opened (solr:{0}, lucene:{1})".format(self.solr_version, self.lucene_version))
