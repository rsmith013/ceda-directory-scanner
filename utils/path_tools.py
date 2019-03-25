"""

"""
__author__ = "Richard Smith"
__date__ = "25 Jan 2019"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"


from ceda_elasticsearch_tools.core.log_reader import SpotMapping
import os
import json
import requests


class PathTools():

    def __init__(self, spot_file=None, moles_mapping=None):
        self.spots = SpotMapping(spot_file=spot_file)
        self.moles_mapping_file = moles_mapping

        if moles_mapping:
            with open(moles_mapping) as reader:
                self.moles_mapping = json.load(reader)
        else:
            self.moles_mapping = None


    def generate_path_metadata(self, path):
        """
        Take path and process it to generate metadata as used in ceda directories index
        :param path:
        :return:
        """
        if not os.path.isdir(path):
            return None,None

        archive_path = self.spots.get_archive_path(path)

        dir_meta = {
            'depth': path.count('/'),
            'dir': os.path.basename(path),
            'path': path,
            'archive_path': archive_path,
            'link': False,
            'type': 'dir'
        }

        if os.path.islink(path) and path is not archive_path:
            dir_meta['link'] = True

        record = self.get_moles_record_metadata(path)

        if record and record["title"]:
            dir_meta["title"] = record["title"]
            dir_meta["url"] = record["url"]
            dir_meta["record_type"] = record["record_type"]

        readme = self.get_readme(path)

        if readme:
            dir_meta["readme"] = readme

        return dir_meta, dir_meta['link']


    def get_moles_record_metadata(self, path):
        orig_path = path
        if self.moles_mapping:

            # recursively check for a match
            while len(path) > 1:
                if path in self.moles_mapping:
                    return self.moles_mapping[path]
                elif path + "/" in self.moles_mapping:
                    return self.moles_mapping[path + "/"]
                else:
                    path = os.path.dirname(path)

        url = "http://catalogue.ceda.ac.uk/api/v0/obs/get_info{}".format(orig_path)
        response = requests.get(url)

        # Update moles mapping file
        if response:
            self.moles_mapping[orig_path] = response.json()
            return response.json()

    def get_readme(self, path):
        if "00README" in os.listdir(path):
            with open(os.path.join(path,"00README")) as reader:
                content = reader.read()

            return content.decode('utf-8','ignore').encode("utf-8")

    def update_moles_mapping(self):
        if self.moles_mapping:
            with open(self.moles_mapping_file,'w') as writer:
                writer.write(json.dumps(self.moles_mapping))
