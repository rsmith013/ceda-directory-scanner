"""
########################################################################################################################

SUBMIT TASKS TO LOTUS

Author: Richard Smith
Email: richard.d.smith@stfc.ac.uk
Date: 10 August 2018

########################################################################################################################

Use this script to run scripts on lotus:
    - generate_dirs_from_spot.py    read the filelist for each spot and generate unique directories

Use the --dev flag to run to code locally rather than sending to lotus

<data_dir> and <output_dir> are required for generate_dirs but not for index_missing


Usage:

lotus_submit.py <output_dir> --config config --generate-dirs [--dev]


Options:

--config            Path to configuration file
--generate-dirs     Flag to indicate to use the generate_dirs script
--dev               Flag to use localhost not lotus


"""
__author__ = "Richard Smith"
__date__ = "25 Jan 2019"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import argparse
import os
import subprocess
import requests
from ConfigParser import ConfigParser

parser = argparse.ArgumentParser(description="Submit script to lotus")

# Setup command line options
parser.add_argument("--config", dest="config", help="Path to configuration file", required=True)
parser.add_argument('--generate-dirs', dest='generate_dirs', action='store_true')
parser.add_argument('--dev', dest='dev', action='store_true')

#################################################
#                                               #
#                Functions                      #
#                                               #
#################################################


def download_spot_mapping():
    # Download the spot mappings from the cedaarchiveapp
    url = "http://cedaarchiveapp.ceda.ac.uk/cedaarchiveapp/fileset/download_conf/"

    response = requests.get(url)
    log_mapping = response.text.split('\n')

    output_list = []
    for line in log_mapping:
        if not line.strip(): continue
        spot, path = line.strip().split()
        output_list.append('{}={}\n'.format(spot, path))

    # Write all spot mappings to file
    with open("spot_mapping.txt", 'w') as output:
        output.writelines(output_list)

def get_spot_paths():

    paths = []
    with open('spot_mapping.txt') as reader:
        mapping = reader.readlines()

    for line in mapping:
        spot, path = line.strip().split('=')
        paths.append(path)

    return paths

#################################################
#                                               #
#                End of Functions               #
#                                               #
#################################################


args = parser.parse_args()

config = ConfigParser()
config.read(args.config)

OUTPUT_DIR = config.get("files","processing-directory")

if not os.path.isdir(OUTPUT_DIR):
    print("Output dir does not exist")
    exit()


# Select script to use
if args.generate_dirs:

    SCRIPT = 'create_dir_index/scripts/generate_dirs_from_spot.py'

else:
    SCRIPT = None
    print("No script can be found. Check your flags and retry")
    exit()

# Use generate dir script
if args.generate_dirs:
    print ("Generating spot mapping...")
    download_spot_mapping()

    print ("Processing spot mapping paths...")
    input_paths = get_spot_paths()

    for path in input_paths:
        cmd = "python {script} {input_path} {output_dir}".format(script=SCRIPT, input_path=path,
                                                                 output_dir=OUTPUT_DIR)
        if args.dev:
            print (cmd)
            subprocess.call(cmd, shell=True)

        else:
            subprocess.call("bsub -q short-serial -e errors/%J.err -W 24:00 {}".format(cmd), shell=True)


