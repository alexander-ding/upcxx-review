from .social import *
from .communities import *
from .currency import *

def get_all_graphs(config):
    parsers = [FacebookCircleParser(config), 
               TwitterCircleParser(config),
               GoogleCircleParser(config), 
               OrkutParser(config), 
               YoutubeParser(config),
               BitcoinOTCParser(config),
               BitcoinAlphaParser(config)]

    for parser in parsers:
        parser.get()
        # parser.cleanup()