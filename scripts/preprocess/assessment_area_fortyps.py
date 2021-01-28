import pathlib

import pandas as pd

from carbonplan_retro.utils import assessment_area_str_to_aa_code


def get_olaf_fortypcs():
    """ARB repo"""
    in_fn = pathlib.Path(__file__).parents[2] / 'data/assessment_area_forest_typs.csv'
    df = pd.read_csv(in_fn)
    df['fortypcds'] = (
        df['rest'].str.extract(r'\(([\d,\s]+)\)')[0].apply(lambda x: [int(y) for y in x.split(',')])
    )
    df['aa_code'] = df['Community'].map(assessment_area_str_to_aa_code())
    df = df[~df.duplicated(subset=['aa_code'])]

    return df.set_index('aa_code')['fortypcds'].to_dict()


if __name__ == '__main__':
    print(get_olaf_fortypcs())
