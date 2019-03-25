"""
########################################################################################################################

INDEX DIRS

Author: Richard Smith
Email: richard.d.smith@stfc.ac.uk
Date: 10 August 2018

########################################################################################################################

Reads directory containing output from generate_dirs_from_spot and creates a unique set of directories.
This set is filtered for items which do not have moles metadata e.g. title and this list is dumped to file for further processing

The remainder is uploaded to elasticsearch.

Usage:

    index_dirs.py <input_dir> --index <index>


"""
__author__ = "Richard Smith"
__date__ = "25 Jan 2019"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import argparse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import os
from tqdm import tqdm
import json
import hashlib
from ConfigParser import ConfigParser

import multiprocessing as mp

parser = argparse.ArgumentParser(description='Collect all dirs together and submit to elasticsearch')
parser.add_argument("--config", dest="config", help="Path to configuration file", required=True)

#################################################
#                                               #
#                Functions                      #
#                                               #
#################################################

def gendata(input_data):
    for item in tqdm(input_data, desc="Building elasticsearch index"):
        item = json.loads(item)
        path = item['path']
        id = hashlib.sha1(path).hexdigest()

        yield {
            "_index": ES_INDEX,
            "_type": "dir",
            "_id": id,
            "_source": item
        }

def load_file(file):
    tree = set()
    with open(os.path.join(INPUT_DIR, file)) as input:
        for line in input:
            tree.add(line.strip())

    return tree

#################################################
#                                               #
#                End of Functions               #
#                                               #
#################################################
args = parser.parse_args()

conf = ConfigParser()
conf.read(args.config)

INPUT_DIR = conf.get("files", "processing-directory")
ES_INDEX = conf.get("elasticsearch", "es-index")

# List input files
print("Listing dirs...")
file_list = os.listdir(INPUT_DIR)

# filter file list
print("Filtering files...")
file_list = [x for x in file_list if x.endswith(".txt")]

# Create set to hold dirs
full_tree = set()

pool = mp.Pool(processes=6)

# Find unique dirs
r = pool.map(load_file, tqdm(file_list, desc="Loading directories"), chunksize=20)
pool.close()
pool.join()

for result in tqdm(r,desc="Processing results"):
    full_tree.update(result)




# Find unique dirs
# for file in tqdm(file_list, desc="Loading dirs"):
#     with open(os.path.join(INPUT_DIR, file)) as input:
#         for line in input:
#             full_tree.add(line.strip())

print("No. dirs: {}".format(len(full_tree)))

# Separate the dirs with missing metadata
missing_metadata = []
complete = []

for dir in tqdm(full_tree, desc="Filtering for missing metadata"):
    if '"title": "' not in dir and '"depth": 1,' not in dir:
        missing_metadata.append(dir)
    else:
        complete.append(dir)

print("Complete: {} Missing: {}".format(len(complete), len(missing_metadata)))

print("Writing dirs missing metadata to file...")
with open(conf.get("files", "missing-metadata-file"), 'w') as missing_file:
    for item in missing_metadata:
        missing_file.write(item + '\n')

# Push complete results to elasticsearch
# Setup elasticsearch connection
es = Elasticsearch([conf.get("elasticsearch", "es-host")],
                   http_auth=(conf.get("elasticsearch", "es-user"),
                              conf.get("elasticsearch", "es-password")
                              )
                   )

# Upload to elasticsearch
bulk(es, gendata(complete))
