import json

import geopandas as gpd
import pandas as pd

def get_proj_series(d):
    ser = pd.Series(d)

    strip = lambda x: x.strip()
    def str_to_tuple(s):
        return tuple(map(strip, s[1:-1].replace("'", "").split(',')))

    ser.index = pd.MultiIndex.from_tuples(map(str_to_tuple, ser.index))
    return ser

class ARBProject(object):
    _data = None
    _geom = None

    def __init__(self, project_id):
        self._proj_id = project_id
        self._load_project()
        self._load_geometry()

    def _load_project(self):
        with open('data/projects.json') as f: # TODO some config magic for these fnames
            all_proj = json.load(f)
            proj_d = all_proj.get(self._proj_id)
            self._data = get_proj_series(proj_d)

    def _load_geometry(self):
        self._geom = gpd.read_file(f"data/geometry/projects/{self._proj_id}.json").iloc[0].geometry
    