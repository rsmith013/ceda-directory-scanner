"""
########################################################################################################################

GENERATE DIRS FROM SPOT

Author: Richard Smith
Email: richard.d.smith@stfc.ac.uk
Date: 17 August 2018

########################################################################################################################

Generate files containing directories and associated metadata. Will follow links which point inside the archive to build
complete directory tree of the archive.

Usage:

    generate_dirs_from_spot.py <dir> <output_dir>

"""
__author__ = "Richard Smith"
__date__ = "25 Jan 2019"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"


import os
from ceda_elasticsearch_tools.core.log_reader import SpotMapping
import json
import argparse

parser = argparse.ArgumentParser(
    description="Walk spots and generate list of directories with MOLES metadata where possible")

parser.add_argument('input_dir', help="Input directory to scan")
parser.add_argument('output_dir', help="Directory to write results to")


#################################################
#                                               #
#                Functions                      #
#                                               #
#################################################

def process_path(dir):
    """
    Process the path and return metadata. Also returns whether the directory is linked to another location in the archive.

    :param dir: direcory path to process

    :return:    dir_meta - dictionary of directory metadata
                link     - boolean describing if the directory links to a location inside the archive
    """

    archive_path = spots.get_archive_path(dir)

    dir_meta = {
        'depth': dir.count('/'),
        'dir': os.path.basename(dir),
        'path': dir,
        'archive_path': archive_path,
        'link': False,
        'type': "dir"
    }

    if os.path.islink(dir) and dir != archive_path:
        dir_meta['link'] = True

    record = get_moles_record_meta(archive_path)
    if record and record["title"]:
        dir_meta["title"] = record["title"]
        dir_meta["url"] = record["url"]
        dir_meta["record_type"] = record["record_type"]

    return dir_meta, dir_meta['link']


def get_moles_record_meta(dir):
    """
    Use the archive path to check the mapping for a match.

    :param dir: directory to test
    :return: MOLES record info for the given dir
    """

    # recursively check for a match
    while len(dir) > 1:
        if dir in moles_mapping:
            return moles_mapping[dir]
        elif dir + "/" in moles_mapping:
            return moles_mapping[dir + "/"]
        else:
            dir = os.path.dirname(dir)


#################################################
#                                               #
#                End of Functions               #
#                                               #
#################################################

# Parse command line arguments
args = parser.parse_args()

SCAN_DIR = args.input_dir

if SCAN_DIR.endswith('/'):
    SCAN_DIR = SCAN_DIR[:-1]

OUTPUT_DIR = args.output_dir

# Setup
output = []
print ("Loading spot mapping...")
spots = SpotMapping(spot_file="spot_mapping.txt")

# Load moles_mapping
print ("Loading MOLES mapping...")
with open('moles_catalogue_mapping.json') as reader:
    moles_mapping = json.load(reader)

# Add the root
root_meta, islink = process_path(SCAN_DIR)
output.append(root_meta)

# Process the tree
print ("Processing tree...")
readmes = {}
for root, dirs, files in os.walk(SCAN_DIR):

    # Check for 00README
    if "00README" in files:
        with open(os.path.join(root,"00README")) as reader:
            content = reader.read()
        readmes[root] = content.decode('utf-8','ignore').encode("utf-8")

    for directory in dirs:
        path = os.path.join(root, directory)

        metadata, islink = process_path(path)
        output.append(metadata)

        # Map directories below link points. Different to followlinks = True above as islink is more selective.
        if islink:
            for link_root, link_dirs, link_files in os.walk(path, followlinks=True):

                # Check for 00README
                if "00README" in link_files:
                    with open(os.path.join(link_root, "00README")) as reader:
                        content = reader.read()
                    readmes[link_root] = content.decode('utf-8','ignore').encode("utf-8")

                for d in link_dirs:
                    in_link_path = os.path.join(link_root, d)

                    metadata, islink = process_path(in_link_path)
                    output.append(metadata)

# Process readmes
print ("Number of readmes: {}".format(len(readmes)))

# Write output file
print ("Writing output...")
output_filename = os.path.join(OUTPUT_DIR, spots.get_spot(SCAN_DIR) + "_directories.txt")
with open(output_filename, "w") as writer:
    writer.writelines(map(lambda x: json.dumps(x)+"\n", output))

# Write readme output
output_filename = os.path.join(OUTPUT_DIR, spots.get_spot(SCAN_DIR) + "_readmes.json")
with open(output_filename, "w") as writer:
    writer.writelines(json.dumps(readmes))
