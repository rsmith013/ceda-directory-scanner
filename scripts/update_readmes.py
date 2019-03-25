"""
########################################################################################################################

UPDATE READMES

Author: Richard Smith
Email: richard.d.smith@stfc.ac.uk
Date: 29 August 2018

########################################################################################################################

Update elasticsearch records with readme content

Usage:

    update_readmes.py <input_dir> --index <index>

"""
__author__ = "Richard Smith"
__date__ = "25 Jan 2019"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import argparse
import json
import os
from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import hashlib
from ConfigParser import ConfigParser

parser = argparse.ArgumentParser(
    description="")
parser.add_argument("--config", dest="config", help="Path to configuration file", required=True)


#################################################
#                                               #
#                Functions                      #
#                                               #
#################################################

def gendata(input):
    for file in tqdm(input, desc="Processing README JSON files"):
        with open(os.path.join(INPUT_DIR, file)) as reader:
            try:
                data = json.load(reader)
            except ValueError:
                tqdm.write("Unable to read: {}".format(file))
                continue

        for item in data:
            id = hashlib.sha1(item).hexdigest()

            yield {
                "_op_type": "update",
                "_index": INDEX,
                "_type": "dir",
                "_id": id,
                "_source": {"readme": data[item]}
            }

#################################################
#                                               #
#                End of Functions               #
#                                               #
#################################################

# Parse command line arguments
args = parser.parse_args()
conf = ConfigParser()
conf.read(args.config)

INDEX = conf.get("elasticsearch", "es-index")
INPUT_DIR = conf.get("files", "processing-directory")

# Setup elasticsearch connection
es = Elasticsearch([conf.get("elasticsearch", "es-host")],
                   http_auth=(conf.get("elasticsearch", "es-user"),
                              conf.get("elasticsearch", "es-password")
                              )
                   )

# Get input file list
files = os.listdir(INPUT_DIR)

# Filter for readme data files
files = [x for x in files if x.endswith(".json")]

# Index readmes using update operation
bulk(es, gendata(files))
