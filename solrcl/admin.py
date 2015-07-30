# -*- coding: utf8 -*-
"""Classes for administration of SOLR cores"""

from solrcl.base import SOLRBase

class SOLRAdmin(SOLRBase):
    """Class with administration methods"""
    def request(self, resource, parameters=None, data=None, dataMIMEType=None):
        """Generic administration request"""
        if parameters is None:
            parameters = {}
        if not resource.startswith('/'):
            resource = "admin/{0}".format(resource)
        return super(SOLRAdmin, self).request(resource,
                                              parameters=parameters,
                                              data=data,
                                              dataMIMEType=dataMIMEType)

    def cores(self):
        """Returns the list of the cores loaded in SOLR instance"""
        return self.request('cores')['status'].keys()

    def reload(self, core):
        """Reloads schema and configuration for specified core"""
        self.logger.info("Reloading core {0}".format(core))
        return self.request('cores', parameters={'action': 'RELOAD', 'core': core})

    def create(self, name, instanceDir, dataDir='data',
               config='solrconfig.xml', schema='schema.xml'):
        """Creates a new core in SOLR instance"""
        self.logger.info("Creating core {0}".format(name))
        params = {
            'action': 'CREATE',
            'name': name,
            'instanceDir': instanceDir,
            'dataDir': dataDir,
            'config': config,
            'schema': schema}
        return self.request('cores', parameters=params)

    def swap(self, core, other):
        """Swaps <core> and <other>"""
        params = {'action': 'SWAP', 'core': core, 'other': other}
        self.logger.info("Swapping cores: {0} <-> {1}".format(core, other))
        return self.request('cores', parameters=params)

    def unload(self, core, deleteInstanceDir=False):
        """Unloads <core> from SOLR instance and optionally removes associated filesystem"""
        params = {'action': 'UNLOAD', 'core': core}
        if deleteInstanceDir:
            params['deleteInstanceDir'] = 'true'
        self.logger.info("Unloading core: {0}".format(core))
        return self.request('cores', parameters=params)
