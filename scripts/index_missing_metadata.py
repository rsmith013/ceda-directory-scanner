"""
################################################

INDEX MISSING METADATA

Author: Richard Smith
Email: richard.d.smith@stfc.ac.uk
Date: 13 August 2018

################################################

Reads directory containing output from generate_dirs_from_spot and creates a unique set of directories.
This set is filtered for items which do not have moles metadata e.g. title and this list is dumped to file for further processing

The remainder is uploaded to elasticsearch.

Usage:

    index_missing_metadata.py <input_file> --index <index>


Variables:

input_file:         A text file containing files which were not attributed on first pass
index:              Elasticsearch index to put results in

"""
__author__ = "Richard Smith"
__date__ = "25 Jan 2019"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import argparse
import requests
import json
import os
from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import hashlib
from ConfigParser import ConfigParser

parser = argparse.ArgumentParser(description="Load dirs missing metadata and try to add metadata to them")
parser.add_argument("--config", dest="config", help="Path to configuration file", required=True)


#################################################
#                                               #
#                Functions                      #
#                                               #
#################################################


def filter_list(list, filter_key):
    """
    Filters list in place and returns the selected items

    :param list: input list to filter in place
    :param filter_key: key to select for extraction

    :return: list of selected data
    """

    selection = []

    for item in tqdm(list, desc="Filtering list on depth"):
        if filter_key in item:
            selection.append(item)

    return selection


def get_moles_record_meta(dir, mapping):
    while len(dir) > 1:
        if dir in mapping:
            return mapping[dir]
        elif dir + "/" in mapping:
            return mapping[dir + "/"]
        else:
            dir = os.path.dirname(dir)


def gendata(input):
    for item in tqdm(input, desc="Building elasticserch index"):
        item = json.loads(item)
        path = item['path']
        id = hashlib.sha1(path).hexdigest()
        yield {
            "_index": ES_INDEX,
            "_type": "dir",
            "_id": id,
            "_source": item
        }


#################################################
#                                               #
#                End of Functions               #
#                                               #
#################################################

# Get command line arguments
args = parser.parse_args()
conf = ConfigParser()
conf.read(args.config)

INPUT_FILE = conf.get("files", "missing-metadata-file")
ES_INDEX = conf.get("elasticsearch", "es-index")
MISSING_MOLES_MAP = conf.get("files", "moles-mapping")

# Read the input file
with open(INPUT_FILE) as missing:
    data = missing.readlines()

# Remove newline characters
data = [d.strip() for d in data]

# Setup
if MISSING_MOLES_MAP:
    with open(MISSING_MOLES_MAP) as reader:
        mapping = json.load(reader)
else:
    mapping = {}

depth = 1
url = "https://catalogue.ceda.ac.uk/api/v0/obs/get_info"
output_list = []

# Navigate top down and try to attribute as many dirs to MOLES catagories as possible
while depth < 5:
    if len(data) == 0:
        break

    sel = filter_list(data, '"depth": {},'.format(depth))

    if depth > 1:
        # Check to see if there is metadata available from MOLES api
        for item in tqdm(sel, desc="Gathering metadata for filter results"):
            item_dict = json.loads(item)
            item_path = item_dict['path']

            # Check to see if there is data in the mapping already
            if item_path not in mapping.keys():

                # If not in mapping check the MOLES api
                r = requests.get(url + item_path)
                try:
                    r_json = r.json()

                except ValueError:
                    continue

                if r_json['title']:
                    path = item_path

                    mapping[path] = r_json

        # Process the input list using the new mappings to see if the list can be reduced
        improved_metadata = []
        remainder = []
        for item in tqdm(data, desc="Updating metadata based on MOLES meta "):
            dir_meta = json.loads(item)
            record = get_moles_record_meta(dir_meta['path'], mapping)
            if record and record['title']:
                dir_meta["title"] = record["title"]
                dir_meta["url"] = record.get("url") if record.get("url") else record.get("uuid")
                dir_meta["record_type"] = record.get("record_type") if record.get("record_type") else record.get("type")

                improved_metadata.append(dir_meta)
            else:
                remainder.append(item)

        # Add improved list of metadata to output_list
        output_list.extend(improved_metadata)

        # reset data list to just the remaining files
        data = remainder


    else:
        # When only 1 deep, there is no MOLES information leave this blank
        output_list.extend(sel)

    # Try the next level
    depth += 1

print("Improved coverage: {} Missing Metadata: {}".format(len(output_list), len(remainder)))

# Add the remainder to the output to index
output_list.extend(data)

print("Writing mapping to file...")
with open('missing_moles_mapping.json', 'w') as writer:
    writer.write(json.dumps(mapping))

# Output remaining data to a file
with open("reduced_missing.txt", 'w') as output:
    for line in remainder:
        output.write(line + '\n')

# Push complete results to elasticsearch
# Setup elasticsearch connection
es = Elasticsearch([conf.get("elasticsearch", "es-host")],
                   http_auth=(conf.get("elasticsearch", "es-user"),
                              conf.get("elasticsearch", "es-password")
                              )
                   )

bulk(es, gendata(output_list))
