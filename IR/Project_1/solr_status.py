import pysolr
import os

url = 'http://34.31.71.250:8983/solr/'
core_name = 'wikipedia'


def delete_core(core_name):
    print(os.system('sudo -u solr ./solr-9.0.0/bin/solr delete -c {core}'.format(core=core_name)))


def create_core(core_name):
    print(os.system('sudo -u solr ./solr-9.0.0/bin/solr create -c {core} -n data_driven_schema_configs'.format(
            core=core_name)))
    
    
#delete_core(core_name)
#create_core(core_name)


solr = pysolr.Solr(url+core_name, always_commit=True, timeout=10)
print(solr.ping())