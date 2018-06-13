###############################################################################
#
# Python script to manage metadata
#
###############################################################################

import zipfile

import requests
from requests.utils import quote

NAV_API_VERSION = 9
METADATA_NAMESPACE = 'cloudera_framework'
FILE_PROPERTIES = 'cloudera/cloudera.properties'


def parse(connection_jar):
    with zipfile.ZipFile(connection_jar, 'r') as f:
        return dict(l.strip().split("=") for l in f.open(FILE_PROPERTIES)
                    if not l.startswith("#") and not l.startswith("\n"))


def uri(properties, path):
    return properties['navigator.url'] + '/api/v' + \
           str(NAV_API_VERSION) + '/' + path


def put(connection_jar, transaction_id, transaction_properties=None,
        transaction_custom_properties=None, transaction_tags=None, flush_all=False):
    properties = parse(connection_jar)
    metadata_bodies = get(connection_jar, transaction_id)
    if len(metadata_bodies) > 0 and 'identity' in metadata_bodies:
        metadata_body = {}
        if flush_all:
            transaction_properties = {} if transaction_properties is None \
                else transaction_properties
            transaction_custom_properties = {} if transaction_custom_properties is None \
                else transaction_custom_properties
            transaction_tags = {} if transaction_tags is None \
                else transaction_tags
        else:
            if transaction_properties is not None:
                transaction_properties.update(
                    metadata_bodies[0]["properties"]
                    if "properties" in metadata_bodies[0] else {})
            if transaction_custom_properties is not None:
                transaction_custom_properties.update(
                    metadata_bodies[0]["customProperties"][METADATA_NAMESPACE]
                    if "customProperties" in metadata_bodies[0] and
                       METADATA_NAMESPACE in metadata_bodies[0]["customProperties"] else {})
            if transaction_tags is not None:
                transaction_tags.extend(
                    metadata_bodies[0]["tags"]
                    if "tags" in metadata_bodies[0] else ())
        if transaction_properties is not None:
            metadata_body["properties"] = transaction_properties
        if transaction_custom_properties is not None:
            metadata_body["customProperties"] = {METADATA_NAMESPACE: transaction_custom_properties}
        if transaction_tags is not None:
            metadata_body["tags"] = transaction_tags
        response = requests.put(uri(properties, "entities/" +
                                    metadata_bodies[0]["identity"]),
                                auth=(properties['navigator.user'],
                                      properties['navigator.password']),
                                json=metadata_body)
        if response.status_code != 200:
            raise Exception("Put returned non OK status [{}]"
                            .format(response.status_code))
    return metadata_bodies


def get(connection_jar, transaction_id):
    properties = parse(connection_jar)
    query = quote('+' + METADATA_NAMESPACE + '.Transaction:"' + transaction_id +
                  '" +type:operation_execution +deleted:(-deleted:true)')
    metadata_bodies = requests.get(uri(properties, 'entities/?query=' + query +
                                       '&limit=100&offset=0'),
                                   auth=(properties['navigator.user'],
                                         properties['navigator.password'])).json()
    metadata_bodies = [metadata_body for metadata_body in metadata_bodies
                       if 'customProperties' in metadata_body and
                       metadata_body['customProperties'] is not None and
                       METADATA_NAMESPACE in metadata_body['customProperties'] and
                       metadata_body['customProperties'][METADATA_NAMESPACE] is not None and
                       'Transaction' in metadata_body['customProperties'][METADATA_NAMESPACE] and
                       metadata_body['customProperties'][METADATA_NAMESPACE]['Transaction'] is not None]
    for metadata_body in metadata_bodies:
        metadata_body['navigatorUrl'] = properties['navigator.url'] + \
                                        '/?view=detailsView&id=' + metadata_body['identity']
    return metadata_bodies
