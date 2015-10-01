from setuptools import setup, find_packages

setup(
	name='SolrCL',
	packages = find_packages(),
        version='0.1.3',
        description='SOLR connection library',
        author='Zaccheo Bagnati',
        author_email='zaccheob@gmail.com',
	install_requires=['requests>=2.2.0']
)
