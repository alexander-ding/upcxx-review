from .base import BaseParser
import numpy as np
import os

page_url = "https://snap.stanford.edu/data/soc-sign-bitcoin-otc.html"

class BitcoinOTCParser(BaseParser):
    def __init__(self, config):
        download_url = "https://snap.stanford.edu/data/soc-sign-bitcoinotc.csv.gz"
        name = "soc-sign-bitcoinotc.txt"
        weighted = True
        super().__init__(config, name, download_url, weighted)


    def get(self):
        input_graph_dir = self._get_if_necessary()
        if input_graph_dir is None:
            return

        import csv
        ids = set()
        edges = []
        id_map = {}
        with open(input_graph_dir, newline='') as f:
            csv_reader = csv.reader(f, delimiter=',')
            for row in csv_reader:
                fro, to, weight, _ = row
                fro, to, weight = int(fro), int(to), int(weight)
                edges.append((fro, to, weight))
                ids.update([fro, to])
        
        id_map = {}
        for i, id in enumerate(ids):
            id_map[id] = i
        
        n_nodes = len(ids)
        self.nodes = [[] for _ in range(n_nodes)]

        for fro, to, weight in edges:
            # graph is directed
            self.nodes[id_map[fro]].append((id_map[to], weight))

        self.write()

page_url = "https://snap.stanford.edu/data/soc-sign-bitcoin-alpha.html"

class BitcoinAlphaParser(BaseParser):
    def __init__(self, config):
        download_url = "https://snap.stanford.edu/data/soc-sign-bitcoinalpha.csv.gz"
        name = "soc-sign-bitcoinalpha.txt"
        weighted = True
        super().__init__(config, name, download_url, weighted)


    def get(self):
        input_graph_dir = self._get_if_necessary()
        if input_graph_dir is None:
            return

        import csv
        ids = set()
        edges = []
        id_map = {}
        with open(input_graph_dir, newline='') as f:
            csv_reader = csv.reader(f, delimiter=',')
            for row in csv_reader:
                fro, to, weight, _ = row
                fro, to, weight = int(fro), int(to), int(weight)
                edges.append((fro, to, weight))
                ids.update([fro, to])
        
        id_map = {}
        for i, id in enumerate(ids):
            id_map[id] = i
        
        n_nodes = len(ids)
        self.nodes = [[] for _ in range(n_nodes)]

        for fro, to, weight in edges:
            # graph is directed
            self.nodes[id_map[fro]].append((id_map[to], weight))

        self.write()

__all__ = ['BitcoinOTCParser', 'BitcoinAlphaParser']