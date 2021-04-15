import json

import fsspec
import pandas as pd

from carbonplan_forest_offsets.data import get_retro_bucket


def get_rfia_slag_co2e_acre(data):
    # convert `biomass` output to metric tons
    sums = data.sum()
    # 44/12 carbon to co2
    return sums['CARB_TOTAL'] / sums['AREA_TOTAL'] * 44 / 12 * 0.90718474


def main():
    data = pd.read_csv('/home/jovyan/rfia/processed_data/297_by_ecosection.csv')

    data['ECOSECTION'] = data['ECOSUBCD'].str[:-1].str.strip()

    lst = ["M261A", "M261B", "M261D"]
    subset = data[(data['CARB_TOTAL'] > 0) & (data['ECOSECTION'].isin(lst))].copy()
    per_ecosection = (
        subset.groupby(['YEAR', 'site', 'ECOSECTION']).apply(get_rfia_slag_co2e_acre).xs(2010)
    )

    records = per_ecosection.unstack(0).T.to_dict()

    fs_prefix, fs_kwargs = get_retro_bucket()
    fn = f'{fs_prefix}/results/southern_cascades_mixed_conifer_by_ecosection.json'
    with fsspec.open(fn, mode='w', **fs_kwargs) as f:
        json.dump(records, f)


if __name__ == '__main__':
    main()
