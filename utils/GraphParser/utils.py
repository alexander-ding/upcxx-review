from os import mkdir
from pathlib import Path

def mkdir_if_necessary(p):
    if not Path(p).exists():
        mkdir(p)