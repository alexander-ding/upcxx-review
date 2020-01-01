from os import mkdir
from pathlib import Path
import requests
import shutil

def mkdir_if_necessary(p):
    if not Path(p).exists():
        mkdir(p)

def download_file(url, outfile):
    with requests.get(url, stream=True) as r:
        with open(outfile, 'wb') as f:
            shutil.copyfileobj(r.raw, f)