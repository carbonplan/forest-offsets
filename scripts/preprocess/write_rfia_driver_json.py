import argparse
import json
import pathlib

import fsspec
import geopandas

from carbonplan_forest_offsets.load.geometry import get_overlapping_states, load_supersections
from carbonplan_forest_offsets.utils import aa_code_to_ss_code, load_arb_fortypcds

PROJECT_SUPERSECTIONS = [
    1,
    2,
    4,
    15,
    18,
    22,
    24,
    25,
    32,
    35,
    37,
    38,
    39,
    41,
    42,
    43,
    44,
    55,
    58,
    60,
    69,
    76,
    79,
    86,
    88,
    94,
    95,
]

if __name__ == '__main__':
    """Builds json file that is readable by R and rFIA scripts for performing proper subsetting/aggregations"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true")
    parser.add_argument(
        "-f", "--filename", help='specify filename for loading alternative geometries'
    )
    args = parser.parse_args()

    arb_fortyps = load_arb_fortypcds()
    aa_ss_dict = aa_code_to_ss_code()

    if args.all:
        project_list = list(range(1, 98))
        out_fn = 'rfia_assessment_areas_all.json'
    else:
        project_list = PROJECT_SUPERSECTIONS
        out_fn = 'rfia_assessment_areas_subset.json'

    if args.filename:
        supersections = geopandas.read_file(args.filename)
        out_fn = f'rfia_assessment_areas_{pathlib.Path(args.filename).stem}.json'

    else:
        supersections = load_supersections(include_ak=False, fix_typos=True)

    store = []
    for aa_id, fortypcds in arb_fortyps.items():
        ss_id = aa_ss_dict.get(aa_id)

        if ss_id == 62:
            # skipping Ouachita Mixed Forest, because it doesnt have a shape!
            continue

        if ss_id in project_list:

            supersection = supersections.loc[supersections['ss_id'] == ss_id]
            record = {
                'assessment_area_id': aa_id,
                'fortypcds': fortypcds,
                'supersection_name': supersection['SSection'].item(),
                'supersection_id': supersection['ss_id'].item(),
                'postal_codes': [
                    postal_code.upper()
                    for postal_code in get_overlapping_states(supersection.geometry.item())
                ],
            }

            store.append(record)

    fn = pathlib.Path(__file__).parents[2] / 'R' / out_fn
    with fsspec.open(fn, mode='w') as f:
        json.dump(store, f, indent=2)
