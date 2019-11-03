import random

from .communities import *
from .currency import *
from .social import *
from .utils import mkdir_if_necessary


def convert_to_weighted(in_p, out_p):
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

def get_all_graphs(config):
    from pathlib import Path
    parsers = [FacebookCircleParser(config), 
               TwitterCircleParser(config),
               GoogleCircleParser(config), 
               OrkutParser(config), 
               # FriendsterParser(config),
               YoutubeParser(config)]

    mkdir_if_necessary(config.get("DEFAULT", "WorkPath"))
    mkdir_if_necessary(config.get("DEFAULT", "RealGraphPath"))
    for parser in parsers:
        parser.get()
        parser.cleanup()
    
    for parser in parsers:
        mkdir_if_necessary(parser.graph_path.parent / "weighted")
        if not (parser.graph_path.parent / "weighted" / parser.name).exists():
            print("Converting {} to weighted".format(parser.graph_path / parser.name))
            convert_to_weighted(parser.graph_path / parser.name, parser.graph_path.parent / "weighted" / parser.name)
