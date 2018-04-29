###############################################################################
#
# Python script to manage metadata
#
###############################################################################

import zipfile

import requests

NAV_API_VERSION = 9
METADATA_NAMESPACE = 'cloudera_framework'
FILE_PROPERTIES = 'cloudera/cloudera.properties'


def nav_uri(properties, path):
    return properties['navigator.url'] + '/api/v' + str(NAV_API_VERSION) + '/' + path


def getMetaData(connection_jar, transaction_id):
    with zipfile.ZipFile(connection_jar, 'r') as f:
        properties = dict(l.strip().split("=") for l in f.open(FILE_PROPERTIES) if not l.startswith("#") and not l.startswith("\n"))
        query = '+' + METADATA_NAMESPACE + '.Transaction:"' + transaction_id + '" +type:operation_execution +deleted:(-deleted:true)'
        metadatas = requests.get(nav_uri(properties, 'entities/?query=' + query + '&limit=100&offset=0'),
                                 auth=(properties['navigator.user'], properties['navigator.password'])).json()
        metadatas = [metadata for metadata in metadatas if 'customProperties' in metadata and metadata['customProperties'] is not None and
                     METADATA_NAMESPACE in metadata['customProperties'] and metadata['customProperties'][METADATA_NAMESPACE] is not None and
                     'Transaction' in metadata['customProperties'][METADATA_NAMESPACE] and
                     metadata['customProperties'][METADATA_NAMESPACE]['Transaction'] is not None]
        for metadata in metadatas:
            metadata['navigatorUrl'] = properties['navigator.url'] + '/?view=detailsView&id=' + metadata['identity']
        return metadatas
    return list()
