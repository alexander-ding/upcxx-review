import os
from pathlib import Path

import numpy as np

from .base import BaseParser, LargeParser

page_url = "https://snap.stanford.edu/data/com-Orkut.html"

class OrkutParser(LargeParser):
    def __init__(self, config):
        download_url = "https://snap.stanford.edu/data/bigdata/communities/com-orkut.ungraph.txt.gz"
        name = "com-orkut.ungraph.txt"
        weighted = False
        directed = False
        super().__init__(config, name, download_url, directed, weighted)

    def _write_side(self, n_nodes, n_edges, inverse):
        input_graph_dir = self.work_path / Path(self.download_url).stem
        
        outpath = self.graph_path / self.name
        print("Writing the parsed graph {} | Inverse {}...".format(outpath, inverse))

        from collections import defaultdict
        edge_count = defaultdict(lambda : 0)
        
        with open(input_graph_dir, 'r') as f:
            # ignore first 4 lines
            for _ in range(5):
                l = f.readline()
            while l != "":
                fro, to = l[:-1].split("\t")
                if inverse:
                    to, fro = fro, to
                fro, to = int(fro), int(to)
                edge_count[fro-1] += 1
                edge_count[to-1] += 1

                l = f.readline()
        
        # first output the n_nodes, n_edges, and offsets
        with open(outpath, "a") as f:
            current_edge = 0
            for i in range(n_nodes):
                f.write("{}\n".format(current_edge))
                current_edge += edge_count[i]

        print("Offsets done. Outputting edges now")

        offset = 500000 # process that many each time
        for i in range(n_nodes // offset + 1):
            min_id = i * offset
            max_id = min((i+1) * offset, n_nodes) # exclusive
            edges = [list() for _ in range(max_id-min_id)]
            with open(input_graph_dir, 'r') as f:
                # ignore first 4 lines
                for _ in range(5):
                    l = f.readline()
                while l != "":
                    fro, to = l[:-1].split("\t")
                    # -1 because edge id starts at 1 there
                    fro, to = int(fro)-1, int(to)-1
                    if inverse:
                        fro, to = to, fro
                   
                    if (min_id <= fro < max_id):
                        edges[fro-min_id].append(to)
                    
                    if (min_id <= to < max_id):
                        edges[to-min_id].append(fro)
                
                    l = f.readline()

            with open(outpath, "a") as f:
                for edge in edges:
                    for neighbor in edge:
                        f.write("{}\n".format(neighbor))

            print("{0:.0%} done".format(i / (n_nodes // offset + 1)))
            
    def write(self):
        input_graph_dir = self._get_if_necessary()
        if input_graph_dir is None:
            return False
        
        outpath = self.graph_path / self.name
        print("Writing the parsed graph {}...".format(outpath))

        n_nodes = 0
        n_edges = 0

        with open(input_graph_dir, 'r') as f:
            # ignore first 4 lines
            for _ in range(5):
                l = f.readline()
            while l != "":
                fro, to = l[:-1].split("\t")
                fro, to = int(fro), int(to)
                # -1 because edge id starts at 1 there
                n_nodes = max(n_nodes, fro, to)
                n_edges += 2 # undirected, so goes both ways
                
                l = f.readline()

        with open(outpath, 'w') as f:
            f.write("{}\n".format(n_nodes))
            f.write("{}\n".format(n_edges))
        
        self._write_side(n_nodes, n_edges, inverse=False)
        self._write_side(n_nodes, n_edges, inverse=True)
        

page_url = "https://snap.stanford.edu/data/com-Friendster.html"

class FriendsterParser(BaseParser):
    def __init__(self, config):
        download_url = "https://snap.stanford.edu/data/bigdata/communities/com-friendster.ungraph.txt.gz"
        name = "com-friendster.ungraph.txt"
        weighted = False
        super().__init__(config, name, download_url, weighted)

    def get(self):
        input_graph_dir = self._get_if_necessary()
        if input_graph_dir is None:
            return False
        
        outpath = self.graph_path / self.name
        print("Writing the parsed graph {}...".format(outpath))

        n_nodes = 0
        n_edges = 0
        from collections import defaultdict
        edge_count = defaultdict(lambda : 0)
        with open(input_graph_dir, 'r') as f:
            # ignore first 4 lines
            for _ in range(5):
                l = f.readline()
            while l != "":
                fro, to = l[:-1].split("\t")
                fro, to = int(fro), int(to)
                # -1 because edge id starts at 1 there
                edge_count[fro-1] += 1
                edge_count[to-1] += 1
                n_nodes = max(n_nodes ,fro, to)
                n_edges += 2 # undirected, so goes both ways
                
                l = f.readline()

        # first output the n_nodes, n_edges, and offsets
        with open(outpath, "w") as f:
            f.write("{}\n".format(n_nodes))
            f.write("{}\n".format(n_edges))

            current_edge = 0
            for i in range(n_nodes):
                f.write("{}\n".format(current_edge))
                current_edge += edge_count[i]

        print("Offsets done. Outputting edges now")

        offset = 500000 # process that many each time
        for i in range(n_nodes // offset + 1):
            min_id = i * offset
            max_id = min((i+1) * offset, n_nodes) # exclusive
            edges = [list() for _ in range(max_id-min_id)]
            with open(input_graph_dir, 'r') as f:
                # ignore first 4 lines
                for _ in range(5):
                    l = f.readline()
                while l != "":
                    fro, to = l[:-1].split("\t")
                    # -1 because edge id starts at 1 there
                    fro, to = int(fro)-1, int(to)-1
                   
                    if (min_id <= fro < max_id):
                        edges[fro-min_id].append(to)
                    
                    if (min_id <= to < max_id):
                        edges[to-min_id].append(fro)
                
                    l = f.readline()

            with open(outpath, "a") as f:
                for edge in edges:
                    for neighbor in edge:
                        f.write("{}\n".format(neighbor))

            print("{0:.0%} done".format(i / (n_nodes // offset + 1)))
        return True

page_url = "https://snap.stanford.edu/data/com-Youtube.html"

class YoutubeParser(BaseParser):
    def __init__(self, config):
        download_url = "https://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz"
        name = "com-youtube.ungraph.txt"
        directed = False
        weighted = False
        
        super().__init__(config, name, download_url, directed, weighted)

    def get(self):
        input_graph_dir = self._get_if_necessary()
        if input_graph_dir is None:
            return False
        
        outpath = self.graph_path / self.name

        n_nodes = 0
        n_edges = 0
        from collections import defaultdict
        
        with open(input_graph_dir, 'r') as f:
            # ignore first 4 lines
            for _ in range(5):
                l = f.readline()
            while l != "":
                fro, to = l[:-1].split("\t")
                fro, to = int(fro), int(to)

                n_nodes = max(n_nodes, fro, to)
                n_edges += 2 # undirected, so goes both ways
                
                l = f.readline()
        
        self.nodes = [[] for _ in range(n_nodes)]

        with open(input_graph_dir, 'r') as f:
            # ignore first 4 lines
            for _ in range(5):
                l = f.readline()
            while l != "":
                fro, to = l[:-1].split("\t")
                fro, to = int(fro), int(to)

                # -1 because edge id starts at 1 there
                self.nodes[fro-1].append(to-1)
                self.nodes[to-1].append(fro-1)
                l = f.readline()

        return True

__all__ = ["OrkutParser", "YoutubeParser", "FriendsterParser"]
