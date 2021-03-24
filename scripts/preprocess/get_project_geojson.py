import json
import pathlib
import random
import time

import requests

from carbonplan_forest_offsets.load.issuance import load_issuance_table

ENDPOINT = 'https://gis.arb.ca.gov/fedarcgis/rest/services/ARBOC_issuance_map/MapServer/0/query'
OUT_DIR = pathlib.Path(__file__).parents[2] / 'data/geometry/projects/rerun'


def get_project_geojson(object_id):
    '''retreives geojson for each project from ARB issuance map MapServer backend'''
    payload = {
        'objectIds': object_id,
        'geometryType': 'esriGeometryPolygon',
        'spatialRel': 'esriSpatialRelIntersects',
        'outFields': '*',
        'returnGeometry': 'true',
        'returnTrueCurves': 'false',
        'geometryPrecision': 4,
        'returnIdsOnly': 'false',
        'returnCountOnly': 'false',
        'returnZ': 'false',
        'returnM': 'false',
        'returnDistinctValues': 'false',
        'returnExtentsOnly': 'false',
        'f': 'geojson',
    }

    r = requests.get(ENDPOINT, params=payload)
    return r


def main():
    issuance_table = load_issuance_table()
    arb_id_to_opr_id = issuance_table.set_index('arb_id')['opr_id'].to_dict()

    for object_id in range(1, 178):
        try:
            r = get_project_geojson(object_id)
            d = r.json()['features'][0]  # un-nest the actual geojson object
            arb_id = d['properties']['arb_id']
            out_fn = OUT_DIR / f"{arb_id_to_opr_id[arb_id]}.json"
            with open(out_fn, 'w') as f:
                json.dump(d, f)
            time.sleep(random.random() * 5)
        except:
            print(f"-- {object_id} failed --")


if __name__ == '__main__':
    main()
