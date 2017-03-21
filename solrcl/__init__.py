# -*- coding: utf8 -*-
"""
:mod:`SolrCL` --- Use SOLR from Python
===================================================================

It defines a SOLR object to use for interacting with SOLR from python code
"""

#Sets version string
import pkg_resources
__version__ = pkg_resources.get_distribution("solrcl").version

from exceptions import SOLRError
from document import SOLRDocumentError, SOLRDocumentWarning, SOLRDocument, SOLRDocumentFactory
from base import SOLRNetworkError, SOLRResponseError, SOLRResponseFormatError, SOLRRequest, SOLRBase
from core import MissingRequiredField, DocumentNotFound, SOLRReplicationError, ThreadError, SOLRCore
from admin import SOLRAdmin
from create import initCore, freeCore, initSlaveSolrCore, SOLRInitError, ExecuteCommandsError
from log import BaseLogFormatter, ExtendedLogFormatter, HttpLogFilter
from solrtype import SOLRType, NotImplementedSOLRTypeWarning, solr2datetime, datetime2solr
from solrfield import SOLRField
from base import DEFAULT_SOLR_DOMAIN, DEFAULT_SOLR_PORT
