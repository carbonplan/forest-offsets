import pathlib

import pandas as pd

from carbonplan_retro.utils import load_aa_codes, load_aa_to_ss_map


def main():
    """ARB repo"""
    in_fn = pathlib.Path(__file__).parents[1] / 'data/assessment_area_forest_typs.csv'
    df = pd.read_csv(in_fn)
    df['fortypcds'] = (
        df['rest'].str.extract(r'\(([\d,\s]+)\)')[0].apply(lambda x: [int(y) for y in x.split(',')])
    )
    df['aa_code'] = df['Community'].map(load_aa_codes())
    df['ss_code'] = df['aa_code'].map(load_aa_to_ss_map())
    df = df[~df.duplicated(subset=['aa_code', 'ss_code'])]
    df = df.set_index(['ss_code', 'aa_code'])
    long_df = (
        pd.DataFrame(df.fortypcds.tolist(), index=df.index)
        .melt(ignore_index=False, value_name='forest_code')['forest_code']
        .dropna()
        .reset_index()
    )
    fn = pathlib.Path(__file__).parents[1] / 'data/ss_aa_fortypcds.csv'
    long_df.to_csv(fn, index=False)


if __name__ == '__main__':
    main()
