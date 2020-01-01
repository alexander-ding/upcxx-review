from configparser import ConfigParser

from GraphParser import get_all_graphs

import os
cws = os.getcwd()
dir_path = os.path.dirname(os.path.realpath(__file__))
if os.path.join(cws, "utils") != dir_path:
    raise Exception("get_graph.py must be run from the top-level directory of the repository")
    
config = ConfigParser()
config.read("config.ini")

get_all_graphs(config)