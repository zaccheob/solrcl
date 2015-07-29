# -*- coding: utf8 -*-
from base import *

class SOLRAdmin(SOLRBase):
	def request(self, resource, parameters={}, data=None, dataMIMEType=None):
		if not resource.startswith('/'):
			resource = "admin/{0}".format(resource)
		return super(SOLRAdmin, self).request(resource, parameters=parameters, data=data, dataMIMEType=dataMIMEType)

	def cores(self):
		return self.request('cores')['status'].keys()

	def reload(self, core):
		self.logger.info("Reloading core {0}".format(core))
		return self.request('cores', parameters={'action': 'RELOAD', 'core': core})

	def create(self, name, instanceDir, dataDir='data', config='solrconfig.xml', schema='schema.xml'):
		self.logger.info("Creating core {0}".format(name))
		params = {'action': 'CREATE', 'name': name, 'instanceDir': instanceDir, 'dataDir': dataDir, 'config': config, 'schema': schema}
		return self.request('cores', parameters=params)

	def swap(self, core, other):
		params = {'action': 'SWAP', 'core': core, 'other': other}
		self.logger.info("Swapping cores: {0} <-> {1}".format(core, other))
		return self.request('cores', parameters=params)

	def unload(self, core, deleteInstanceDir):
		params = {'action': 'UNLOAD', 'core': core}
		if deleteInstanceDir:
			params['deleteInstanceDir'] = 'true'
		self.logger.info("Unloading core: {0}".format(core))
		return self.request('cores', parameters=params)
