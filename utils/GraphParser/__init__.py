from .social import *
from .communities import *
from .currency import *
from .utils import mkdir_if_necessary

def get_all_graphs(config):
    from pathlib import Path
    parsers = [FacebookCircleParser(config), 
               TwitterCircleParser(config),
               GoogleCircleParser(config), 
               OrkutParser(config), 
               YoutubeParser(config),
               BitcoinOTCParser(config),
               BitcoinAlphaParser(config)]

    mkdir_if_necessary(config.get("DEFAULT", "WorkPath"))
    mkdir_if_necessary(config.get("DEFAULT", "RealGraphPath"))
    for parser in parsers:
        parser.get()
        parser.cleanup()