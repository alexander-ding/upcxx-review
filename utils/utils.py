import os
import random
import shutil
from configparser import ConfigParser
from os import mkdir
from pathlib import Path

import requests

GRAPH_PATH = None
CODE_PATH = None
UTILS_PATH = None

def init():
    config = ConfigParser()
    config.read("config.ini")
    global GRAPH_PATH
    global CODE_PATH
    global UTILS_PATH

    GRAPH_PATH = Path(config.get("DEFAULT", "GraphPath"))
    CODE_PATH = Path(config.get("DEFAULT", "CodePath"))
    UTILS_PATH = Path(config.get("DEFAULT", "CodePath")).parent / "utils" / "ligra" / "utils"

init()

def check_cwd():
    cws = os.getcwd()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if os.path.join(cws, "utils") != dir_path:
        raise Exception("generate_power_law_graphs.py must be run from the top-level directory of the repository")


def mkdir_if_necessary(p):
    if not Path(p).exists():
        mkdir(p)

def download_file(url, outfile):
    with requests.get(url, stream=True) as r:
        with open(outfile, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

def url_to_name(url):
    return Path(url).stem


def add_weights_graph(in_p, out_p):
    with open(in_p) as fin:
        n = int(fin.readline()[:-1])
        m = int(fin.readline()[:-1])
        with open(out_p, 'w') as fout:
            fout.write("{}\n".format(n))
            fout.write("{}\n".format(m))
            for _ in range(n):
                fout.write(fin.readline())
            
            for _ in range(m):
                to_index = int(fin.readline()[:-1])
                weight = random.randint(0, 10)
                fout.write("{} {}\n".format(to_index, weight))

            for _ in range(n):
                fout.write(fin.readline())
            
            for _ in range(m):
                to_index = int(fin.readline()[:-1])
                weight = random.randint(0, 10)
                fout.write("{} {}\n".format(to_index, weight))
