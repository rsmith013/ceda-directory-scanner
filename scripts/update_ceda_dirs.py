"""
Script to use to maintain ceda directory index which is back end of archive browser

Useage:

    ceda_dirs.py -h | --help
    ceda_dirs.py <index> -d <log_directory> --moles-catalogue-mapping <moles_mapping>

Options:
    -d      Directory to keep a history of the logfiles scanned

"""
__author__ = "Richard Smith"
__date__ = "25 Jan 2019"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import argparse
from ceda_elasticsearch_tools.core.log_reader import DepositLog, SpotMapping
from ceda_elasticsearch_tools.index_tools.index_updaters import CedaDirs
from ceda_elasticsearch_tools.core.utils import get_latest_log
from utils.path_tools import PathTools
from tqdm import tqdm
import os
import hashlib
import sys
from ConfigParser import ConfigParser

parser = argparse.ArgumentParser(
    description="Script to use to maintain ceda directory index which is back end of archive browser"
)

parser.add_argument("--conf", dest="conf", required=True)


#################################################
#                                               #
#                Functions                      #
#                                               #
#################################################

def make_logging_dir(directory):
    """
    Make directory if it doesn't already exist
    :param directory: Path to create
    """

    if not os.path.isdir(directory):
        os.makedirs(directory)


def check_logging_dir(directory, log):
    """
    Check to see if the log has been processed already.
    :param directory:   Logging directory to test
    :param log:         Log name to be processed
    :return:            Bool, logging path
    """

    log_root = os.path.splitext(log)[0]
    action_output_filename = "{}_CEDA_DIRS_REPORT.txt".format(log_root)
    action_output = os.path.join(directory, action_output_filename)

    if action_output_filename in os.listdir(directory):
        return True, action_output

    else:
        return False, action_output


#################################################
#                                               #
#                End of Functions               #
#                                               #
#################################################


def main():
    """
    Process the logfiles and update elasticsearch index


    :return:
    """
    # Get command line arguments
    args = parser.parse_args()

    conf = ConfigParser()
    conf.read(args.conf)

    # Get the latest logs
    deposit_logs = get_latest_log("/badc/ARCHIVE_INFO/deposit_logs", "deposit_ingest", rank=-2)
    # deposit_logs = ['deposit_ingest1.ceda.ac.uk_20180824.log']

    # Check to see if logging directory exists
    make_logging_dir(conf.get("files", "status-directory"))

    # Initialise ceda dirs updater
    cd = CedaDirs(index=conf.get("elasticsearch", "es-index"), host_url=conf.get("elasticsearch", "es-host"), **{
        "http_auth": (
            conf.get("elasticsearch", "es-user"),
            conf.get("elasticsearch", "es-password")
        )
    })

    # Prepare path tools
    if conf.get("files", "moles-mapping"):
        pt = PathTools(moles_mapping=conf.get("files", "moles-mapping"))
    else:
        pt = PathTools()

    for log in deposit_logs:

        # Read deposit logs
        dl = DepositLog(log_filename=log)

        # Check to see if log has already been processed
        processed, logging_path = check_logging_dir(conf.get("files", "status-directory"), log)

        # Skip processing if log has already been processed
        if processed:
            continue

        #################################################
        #                                               #
        #         Process directory creations           #
        #                                               #
        #################################################
        content_list = []
        result_list = []

        for dir in tqdm(dl.mkdir_list, desc="Processing creations", file=sys.stdout):
            metadata, islink = pt.generate_path_metadata(dir)
            if metadata:
                content_list.append({
                    "id": hashlib.sha1(metadata["path"]).hexdigest(),
                    "document": metadata
                })

        result = cd.add_dirs(content_list)

        result_list.append(
            "New dirs: {} Operation status: {}".format(
                len(dl.mkdir_list),
                result
            )
        )

        #################################################
        #                                               #
        #         Process directory deletions           #
        #                                               #
        #################################################
        deletion_list = []

        for dir in tqdm(dl.rmdir_list, desc="Processing deletions", file=sys.stdout):
            deletion_list.append({"id": hashlib.sha1(dir).hexdigest()})

        result = cd.delete_dirs(deletion_list)

        result_list.append(
            "Deleted dirs: {} Operation status: {}".format(
                len(dl.rmdir_list),
                result
            )
        )

        #################################################
        #                                               #
        #               Process symlinks                #
        #                                               #
        #################################################
        # If there are symlink actions in the deposit log. Process the directory as if
        # it is a new directory.
        content_list = []

        for dir in tqdm(dl.symlink_list, desc="Processing symlinks", file=sys.stdout):
            metadata, islink = pt.generate_path_metadata(dir)
            if metadata:
                content_list.append({
                    "id": hashlib.sha1(metadata["path"]).hexdigest(),
                    "document": metadata
                })

        result = cd.add_dirs(content_list)

        result_list.append(
            "Symlinked dirs: {} Operation status: {}".format(
                len(dl.symlink_list),
                result
            )
        )

        #################################################
        #                                               #
        #               Process 00READMEs               #
        #                                               #
        #################################################
        content_list = []

        for readme in tqdm(dl.readme00_list, desc="Processing 00READMEs", file=sys.stdout):
            path = os.path.dirname(readme)
            content = pt.get_readme(path)
            if content:
                content_list.append({
                    "id": hashlib.sha1(path).hexdigest(),
                    "document": {"readme": content}
                })

        result = cd.update_readmes(content_list)

        result_list.append(
            "Added 00READMEs: {} Operation status: {}".format(
                len(dl.readme00_list),
                result
            )
        )

        #################################################
        #                                               #
        #               Write log file                  #
        #                                               #
        #################################################
        # Write log file
        with open(logging_path, 'w') as writer:
            writer.writelines(map(lambda x: x + "\n", result_list))

    #################################################
    #                                               #
    #               Process Spot Roots              #
    #                                               #
    #################################################

    spot_log = SpotMapping()
    spot_paths = spot_log.path2spotmapping.keys()

    content_list = []

    for spot in tqdm(spot_paths, desc="Processing spot roots", file=sys.stdout):
        metadata, islink = pt.generate_path_metadata(spot)
        if metadata:
            content_list.append({
                "id": hashlib.sha1(metadata["path"]).hexdigest(),
                "document": metadata
            })

    result = cd.add_dirs(content_list)

    pt.update_moles_mapping()

    print("Spot dirs: {} Operation status: {}".format(
        len(spot_log),
        result
    ))


if __name__ == "__main__":
    main()
