""" Downloads and converts SNAP graphs
"""
import argparse
import os
import subprocess
from pathlib import Path

from generate_graph import convert_graph
from utils import (GRAPH_PATH, UTILS_PATH, add_weights_graph, check_cwd,
                   download_file, mkdir_if_necessary, url_to_name)


class Parser:
    def __init__(self, url):
        self.name = url_to_name(url)
        self.url = url

    @property
    def raw_file(self):
        return GRAPH_PATH / "real"/ "raw" / self.name

    @property
    def temporary_file(self):
        return GRAPH_PATH / "real"/ f"{self.name}_temp"

    @property
    def unweighted_parsed_file(self):
        return GRAPH_PATH / "real"/  "unweighted" / self.name

    @property
    def weighted_parsed_file(self):
        return GRAPH_PATH / "real"/  "weighted" / self.name

    def parse(self):
        if not self.unweighted_parsed_file.exists():
            self._parse_unweighted()
            self._parse_weighted()
            os.remove(self.temporary_file)
        
    def _parse_weighted(self):
        add_weights_graph(self.unweighted_parsed_file, self.weighted_parsed_file)

    def _parse_unweighted(self):
        self._get_if_necessary()
        command = f"{UTILS_PATH}/SNAPtoAdj -s {self.raw_file} {self.temporary_file}"
        print(command)
        output = subprocess.check_output(command, shell=True)
        convert_graph(self.temporary_file, self.unweighted_parsed_file)
    
    def _get_if_necessary(self):
        """ Gets graph from online or loads the local copy and
            returns the path to the unzipped file.
        """

        if self.raw_file.exists():
            return
        
        download_path = self.raw_file.parent / Path(self.url).parts[-1]
        if not download_path.exists():
            print("Preexisting file {} not found. Downloading to {}...".format(self.raw_file, download_path))
            download_file(self.url, download_path)
            print("...Done")
    
        self._unzip(download_path)
        

    def _unzip(self, f):
        """ Unzips the downloaded file
        """
        print("Extracting {}".format(f.absolute()))
        os.system("gzip -d {}".format(f.absolute()))


urls = ["https://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz", "https://snap.stanford.edu/data/bigdata/communities/com-orkut.ungraph.txt.gz", "https://snap.stanford.edu/data/bigdata/communities/com-friendster.ungraph.txt.gz"]

def main():
    graph_path = GRAPH_PATH / "real"
    mkdir_if_necessary(graph_path)
    mkdir_if_necessary(graph_path / "unweighted")
    mkdir_if_necessary(graph_path / "weighted")
    mkdir_if_necessary(graph_path / "raw")

    for url in urls:
        print(url)
        p = Parser(url)
        p.parse()
    

if __name__ == "__main__":
    check_cwd()

    main()
