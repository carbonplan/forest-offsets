import argparse
import json
import os

import fsspec

from carbonplan_retro.load.arb_fortypcds import load_arb_fortypcds
from carbonplan_retro.load.geometry import get_overlapping_states, load_supersections
from carbonplan_retro.utils import aa_code_to_ss_code

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true")

    args = parser.parse_args()

    arb_fortyps = load_arb_fortypcds()
    aa_ss_dict = aa_code_to_ss_code()

    if args.all:
        supersection_list = list(range(1, 98))
        out_fn = 'rfia_assessment_areas_all.json'
    else:
        supersection_list = PROJECT_SUPERSECTIONS
        out_fn = 'rfia_assessment_areas_subset.json'

    store = []
    for supersection_id in supersection_list:

        supersections = load_supersections(include_ak=False, fix_typos=True)

        supersection = supersections.loc[supersections['ss_id'] == supersection_id]
        record = {
            'supersection_name': supersection['SSection'].item(),
            'supersection_id': supersection['ss_id'].item(),
            'postal_codes': [
                postal_code.upper()
                for postal_code in get_overlapping_states(supersection.geometry.item())
            ],
        }

        store.append(record)

    fn = os.path.join('/home/jovyan/lost+found/', out_fn)
    with fsspec.open(fn, mode='w') as f:
        json.dump(store, f, indent=2)
