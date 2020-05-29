# -*- coding: utf8 -*-
import os
import logging
import pipes
import subprocess
import socket

import exceptions
from base import *
import admin

#Create a custom logger
logger = logging.getLogger("solrcl")
logger.setLevel(logging.DEBUG)

MINIMAL_REPLICABLE_SOLRCONFIG = """<?xml version="1.0" encoding="UTF-8" ?>
<config>
  <luceneMatchVersion>LUCENE_41</luceneMatchVersion>
  <requestHandler name="/admin/" class="org.apache.solr.handler.admin.AdminHandlers" />
  <requestHandler name="/replication" class="solr.ReplicationHandler" startup="lazy">
    <lst name="slave">
      <str name="enable">true</str>
      <str name="masterUrl">http://${wki.master.address}/solr/${wki.master.core.name}</str>
    </lst>
  </requestHandler>
  <requestHandler name="/select" class="solr.SearchHandler" />
</config>"""

MINIMAL_SCHEMA = """<?xml version="1.0" ?>
<schema version="1.5">
</schema>"""

SOLRCORE_PROPERTIES = """enable.master=false
enable.slave=true
wki.master.address={solr_domain}:{solr_port}
wki.master.core.name={master_core}"""


class ExecuteCommandsError(Exception):
	def __init__(self, message, rc=0):
		Exception.__init__(self, message)
		self.rc = rc

class SOLRInitError(exceptions.SOLRError): pass


def _executeCommands(commands, ssh_user=None, domain=DEFAULT_SOLR_DOMAIN):
	for command in commands:
		shcommand = ' '.join(command)
		logger.debug(shcommand)

		try:
			cwd = os.getcwd()
		except OSError:
			cwd = "/tmp"

		#if domain != localhost requires ssh access
		if domain == 'localhost' or domain == socket.gethostname() or domain == socket.gethostbyname(socket.gethostname()) or domain == socket.getfqdn():
			#Local
			p = subprocess.Popen(shcommand, shell=True, cwd=cwd)
		else:
			#Remote
			p = subprocess.Popen(('ssh', '{0}@{1}'.format(ssh_user, domain), shcommand), shell=False, cwd=cwd)

		rc = p.wait()
		if rc != 0:
			raise ExecuteCommandsError("Process {0} exited with return code {1} on host {2}".format(shcommand, rc, domain), rc=rc)


def initCore(core, instance_dir, solrconfig, schema, domain=DEFAULT_SOLR_DOMAIN, port=DEFAULT_SOLR_PORT, ssh_user=None, additional_conf_files=()):
	commands = []
	commands.append(('mkdir', '-p', '-m', '775', pipes.quote('{0}/data'.format(instance_dir))))
	commands.append(('mkdir', '-p', '-m', '775', pipes.quote('{0}/conf'.format(instance_dir))))
	for (fn, content) in (('solrconfig.xml', solrconfig), ('schema.xml', schema)) + tuple(additional_conf_files):
		commands.append(('[', '-f', pipes.quote('{0}/conf/{1}'.format(instance_dir, fn)), ']', '&&', 'exit', '2', ';', 'true'))
		commands.append(('echo', pipes.quote(content), '>', pipes.quote('{0}/conf/{1}'.format(instance_dir, fn))))

	try:
		_executeCommands(commands, ssh_user=ssh_user, domain=domain)
	except ExecuteCommandsError, err:
		if err.rc == 2:
			raise SOLRInitError, "Core already exists in directory {0} on host {1}".format(instance_dir, domain)
		else:
			raise
			

	sa = admin.SOLRAdmin(domain=domain, port=port)
	sa.create(core, instance_dir)


def freeCore(core, domain=DEFAULT_SOLR_DOMAIN, port=DEFAULT_SOLR_PORT, ssh_user=None):
	sa = admin.SOLRAdmin(domain=domain, port=port)
	sa.unload(core, deleteInstanceDir=True)


def initSlaveSolrCore(core, instance_dir, master_core, domain=DEFAULT_SOLR_DOMAIN, port=DEFAULT_SOLR_PORT, ssh_user=None, master_domain=DEFAULT_SOLR_DOMAIN, master_port=DEFAULT_SOLR_PORT):
	additional_conf_files = (('solrcore.properties', SOLRCORE_PROPERTIES.format(solr_domain=master_domain, solr_port=master_port, master_core=master_core)),)
	initCore(core, instance_dir, MINIMAL_REPLICABLE_SOLRCONFIG, MINIMAL_SCHEMA, domain=domain, port=port, ssh_user=ssh_user, additional_conf_files=additional_conf_files)
