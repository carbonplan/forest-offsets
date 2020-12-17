import json
from itertools import chain

import geopandas as gpd
import pandas as pd

# from utils import load_borders

WORKING_CRS = 'PROJCRS["NAD_1983_Albers",BASEGEOGCRS["NAD83",DATUM["North American Datum 1983",ELLIPSOID["GRS 1980",6378137,298.257222101,LENGTHUNIT["metre",1]]],PRIMEM["Greenwich",0,ANGLEUNIT["Degree",0.0174532925199433]],ID["EPSG",4269]],CONVERSION["unnamed",METHOD["Albers Equal Area",ID["EPSG",9822]],PARAMETER["Easting at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",8827]],PARAMETER["Longitude of false origin",-96,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude of 1st standard parallel",29.5,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",45.5,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Latitude of false origin",23,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]]],CS[Cartesian,2],AXIS["(E)",east,ORDER[1],LENGTHUNIT["metre",1,ID["EPSG",9001]]],AXIS["(N)",north,ORDER[2],LENGTHUNIT["metre",1,ID["EPSG",9001]]]]'


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
        self._load_project_data()
        self._load_geometry()

    def _load_project_data(self):
        with open(
            'data/projects.json'
        ) as f:  # TODO some config magic to make fnames relative to repo, not my machine
            all_proj = json.load(f)
            proj_d = all_proj.get(self._proj_id)
            self._data = get_proj_series(proj_d)

    def _load_geometry(self):
        self._geom = (
            gpd.read_file(f"data/geometry/projects/{self._proj_id}.json")
            .to_crs(WORKING_CRS)
            .iloc[0]
            .geometry
        )

    @property
    def proj_id(self):
        return self._proj_id  # @Joe, how bad is this?

    @property
    def supersections(self):
        return self._data['project']['super_section'].item()

    @property
    def acreage(self):
        return self._data['project']['acreage'].item()

    @property
    def common_practice(self):
        return self._data['baseline']['common_practice'].item()

    @property
    def initial_carbon(self):
        return self._data['baseline']['initial_carbon_stock'].item()

    @property
    def geom(self):
        return self._geom

    @property
    def species_lst(self):
        return self._get_species_lst()

    def _get_species_lst(self):
        try:
            return list(
                set(
                    [
                        spp["code"]
                        # item() exposes dict, as opposed to np.array(dict)
                        for val in (self._data['project']["species"].item()).values()
                        for spp in val
                    ]
                )
            )
        except AttributeError:
            return []

        except:
            raise

    @property
    def assessment_areas(self):
        """"""
        return self._data["project"]["assessment_areas"].item()

    @property
    def border_lst(self):
        return self._get_bordering_ss()

    def _get_bordering_ss(self):
        """
        ss_codes is a list...this is ugly
        """
        try:
            with open("data/border_dict.json") as f:
                borders_d = json.load(f)
            # TODO: this str/float bs needs to get fixed in preprocess step
            lsts = [borders_d.get(str(x)) for x in self.supersections]
            merged = list(set(chain(*lsts)))
            return merged
        except FileNotFoundError:
            raise FileNotFoundError("run preprocessing script so data/border_dict.json exists!")
        except:
            raise
