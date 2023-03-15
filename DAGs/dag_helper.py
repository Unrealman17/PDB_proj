import json
from pathlib import PurePath
import sys
import os
import traceback

def init_project(project: str):
#    raise Exception(os.getcwd())
    with open('projects.json') as f:
        path = json.load(f)[project]
    sys.path.append(path)
    return path


class Chdir:

    def __init__(self, dir, mkdir=False) -> None:
        self.dir = dir
        self.cwd = os.getcwd()
        self.path = PurePath(dir)
        self.isdir = os.path.isdir(self.path)
        if mkdir and not self.isdir:
            os.mkdir(self.path)

    def __enter__(self):
        if self.isdir:
            os.chdir(self.path)
        else:
            print(self.dir, 'not found')
        return self.isdir

    def __exit__(self, exc_type, exc_value, tb):
        os.chdir(self.cwd)
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
