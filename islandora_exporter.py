import sys
import os
import urllib.request
import requests
import urllib.parse
import json
import mimetypes

mimetypes.init()
collection_pid = sys.argv[4]
output_directory = sys.argv[3]
metadata_datastream = sys.argv[2]
site_base_url = sys.argv[1]
site_rest_url = site_base_url + '/islandora/rest/v1/'

def fetch_children(collection_pid, parent_directory):
    children_query = 'RELS_EXT_isMemberOfCollection_uri_t:"' + collection_pid + '"?sort=fgs_label_s+asc&fl=PID,fgs_label_t,RELS_EXT_hasModel_uri_t&rows=1000000'
    children_request_url = site_rest_url + 'solr/' + children_query

    children_response_body = requests.get(children_request_url).text
    children_response_body_dict = json.loads(children_response_body)
    children = children_response_body_dict['response']['docs']

    for child in children:
        if child['RELS_EXT_hasModel_uri_t'] == 'info:fedora/islandora:collectionCModel':
            # need to check for duplicates here
            new_parent = os.path.join(parent_directory, child['fgs_label_t'])
            os.makedirs(new_parent)
            fetch_children(child['PID'], new_parent)

        else:
            # this won't work with compound objects currently
            obj_url = site_base_url + '/islandora/object/' + child['PID'] + '/datastream/OBJ/download'
            print("Downloading " +  site_base_url + "/islandora/object/" + child['PID'])
            obj = requests.get(obj_url)
            mimetype = obj.headers["content-type"]
            extension = mimetypes.guess_extension(mimetype)
            obj_content_path = os.path.join(output_directory, (child['fgs_label_t'] + extension))
            print(obj_content_path)
            with open(obj_content_path, 'wb') as file:
                file.write(obj.content)

        metadata_query = 'solr/PID:' + child['PID'].replace(':', '%3A')
        metadata_url = site_rest_url + 'solr/PID:' + metadata_query
        metadata_url = site_base_url + '/islandora/object/' + child['PID'] + '/datastream/' + metadata_datastream + '/download'
        metadata_xml = urllib.request.urlopen(metadata_url).read().decode('utf-8')
        metadata_xml_path = os.path.join(parent_directory, (child['fgs_label_t'] + ".xml"))
        with open(metadata_xml_path, 'w', encoding="utf-8") as file:
            file.write(metadata_xml)

if __name__ == "__main__":
    fetch_children(collection_pid, output_directory)
