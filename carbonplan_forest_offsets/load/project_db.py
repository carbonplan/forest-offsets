import json
from pathlib import Path

import gspread
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

LOCAL_DATA_PATH = Path(__file__).parents[2] / 'data'
SECRET_FILE = Path(__file__).parents[2] / 'secrets/google-sheets-key.json'


def load_project_db(fn=None, save=True, use_cache=True):
    if use_cache and fn:
        try:
            print(f'loading load {fn} from {LOCAL_DATA_PATH}')
            cache_fn = LOCAL_DATA_PATH / fn
            return load_project_db_from_disk(fn=cache_fn)
        except:
            print(f'failed to load from disk -- grabbing {fn} from google')
            return load_project_db_from_sheets(fn=fn, save=save)
    else:
        return load_project_db_from_sheets(fn=fn, save=save)


def load_project_db_from_disk(fn=None):
    def str_to_tuple(s):
        strip = lambda x: x.strip()
        return tuple(map(strip, s[1:-1].replace("'", "").split(',')))

    if Path(fn).suffix != '.json':
        fn = Path(fn).parent / (Path(fn).name + '.json')
    try:
        df = pd.read_json(fn, orient='index', convert_dates=True)
        df.columns = pd.MultiIndex.from_tuples(map(str_to_tuple, df.columns))
    except ValueError:
        raise ValueError("malformed (or missing) json file")
    return df


def get_sheet(sheet, doc, keypath=None):
    """
    helper function to open a specific google sheet
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    if not keypath:
        keypath = SECRET_FILE

    credentials = ServiceAccountCredentials.from_json_keyfile_name(keypath, scope)

    gc = gspread.authorize(credentials)
    wks = gc.open(doc)
    sheet = wks.worksheet(sheet)
    return sheet


def get_df(sheet):
    data = sheet.get_all_values()
    data = np.asarray(data)
    df = pd.DataFrame(data[1:], columns=data[0])
    # ffill sparse to dense index.
    levels = ['level0', 'level1', 'level2']
    left = df[levels].copy()
    left[levels[:2]] = left[levels[:2]].mask(left == '', None).ffill()
    index = pd.MultiIndex.from_frame(left)

    types = df['type']

    df.index = index
    df = df.drop(columns=levels + ['type'])
    df = df.transpose()
    df = df.iloc[1:]

    types.index = index

    return df, types


def json_loads(v):
    try:
        if 'SEE NOTE' in v:
            return None
        return json.loads(v)
    except:
        print(v)
        raise


def cast_col(col, type_str):
    if type_str == 'YYYY-MM-DD':
        return pd.to_datetime(col, errors='coerce')
    elif type_str == 'str':
        return col.astype(str)
    elif type_str == '[str]':
        return [json_loads(v) if v else [] for v in col]
    elif type_str == 'bool':
        return col.astype(bool)  # col.replace('', '0').astype(int).astype(bool)
    elif type_str == 'int':
        return pd.to_numeric(col.str.replace(',', ''), errors='coerce', downcast='integer')
    elif type_str == 'float':
        return pd.to_numeric(col.str.replace(',', ''), errors='coerce')
    elif type_str == '[lon:float, lat:float]' or type_str == '[int]':
        return [json_loads(v) if v else [] for v in col]
    elif type_str == '[(is_intentional, size)]':
        return col  # TODO
    else:
        try:
            return [json_loads(v) if v else "" for v in col]
        except:
            print(col)
            raise


def load_project_db_from_sheets(fn=None, save=True):
    sheet = get_sheet("ifm", fn)

    df, types = get_df(sheet)

    for index, col in df.iteritems():
        type_str = types[index]
        df[index] = cast_col(col, type_str)
    if save:
        out_path = LOCAL_DATA_PATH / f'{fn}.json'
        df.to_json(out_path, orient='index', date_format='iso', date_unit='s', indent=2)
    return df
