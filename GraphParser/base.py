from pathlib import Path
import requests
import os
from .utils import mkdir_if_necessary

class BaseParser:
    def __init__(self, config, name, download_url, weighted=None):
        self.nodes = []
        self.work_path = Path(config.get("DEFAULT", "WorkPath"))
        graph_path = Path(config.get("DEFAULT", "RealGraphPath"))
        self.graph_path = graph_path / "weighted" if weighted else graph_path / "unweighted"
        self.name = name
        self.download_url = download_url
        self.weighted = False # to be set
        if weighted is not None:
            self.weighted = weighted
    
    def _get_if_necessary(self):
        """ Gets graph from online or loads the local copy and 
            returns the path to the downloaded zip file. 
        """

        if self.parsed:
           print("Parsed file {} exists, skipping".format(self.graph_path / self.name))
           return None

        download_path = self.work_path / Path(self.download_url).parts[-1]
        extracted_path = self.work_path / Path(self.download_url).stem
        if extracted_path.exists():
            return extracted_path 
        print("Preexisting file {} not found. Downloading...".format(extracted_path))
        r = requests.get(self.download_url)
        
        with open(download_path, 'wb') as fp:
            fp.write(r.content)

        self._unzip(download_path)
        print("...Done")
        return extracted_path

    def _unzip(self, f):
        """ Unzips the downloaded file
        """
        os.system("gzip -d {}".format(f.absolute()))

    @property
    def parsed(self):
        outpath = self.graph_path / self.name
        return outpath.exists()

    def get(self):
        """ Gets the graph, downloading if necessary, and initializes
            the class with the graph
        """
        raise NotImplementedError()

    def write(self):
        """ Outputs the graph into a file
        """
        outpath = self.graph_path / self.name
        print("Writing the parsed graph {}...".format(outpath))
        mkdir_if_necessary(outpath)
        with open(outpath, "w") as f:
            n = len(self.nodes)
            m = sum([len(node) for node in self.nodes])
            f.write("{}\n".format(n))
            f.write("{}\n".format(m))
            
            current_edge = 0
            for node in self.nodes:
                f.write("{}\n".format(current_edge))
                current_edge += len(node)

            for node in self.nodes:
                for edge in node:
                    if self.weighted:
                        f.write("{} {}\n".format(edge[0], edge[1]))
                    else:
                        f.write("{}\n".format(edge))
            
        print("...Done")
    
    def cleanup(self):
        """ Cleans the temporary files, if existing
        """
        download_path = self.work_path / self.name
        if download_path.exists():
            os.system("rm {}".format(download_path.as_posix()))