from configparser import ConfigParser

from GraphParser import get_all_graphs

config = ConfigParser()
config.read("config.ini")

get_all_graphs(config)