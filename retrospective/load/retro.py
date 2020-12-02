import gspread
import json
import numpy as np
import os

from oauth2client.service_account import ServiceAccountCredentials
from pandas import DataFrame, MultiIndex, to_datetime, to_numeric, read_json

LOCAL_RETRO_FN = Path(__file__).parent / 'data/local_retro.csv'


def retro(fn=None, save=True):
    try:
        df = load_retro_from_disk(fn=fn)
    except:
        if not fn:
            raise FileNotFoundError(f"failed to load from disk -- specify google fname")
        df = load_retro_from_sheets(fn=fn)
        if save:
            df.to_csv(LOCAL_RETRO_FN)
    return df


def load_retro_from_disk(fn=None):
    if not fn:  # load from cache
        return pd.read_csv(LOCAL_RETRO_FN)
    return pd.read_csv(fn)


def get_sheet(sheet, doc, keypath=None):
    """
    helper function to open a specific google sheet
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    if not keypath:
        keypath = os.expanduser("~/.config/googleapis/carbonplan-key.json")

    credentials = ServiceAccountCredentials.from_json_keyfile_name(keypath, scope)

    gc = gspread.authorize(credentials)
    wks = gc.open(doc)
    sheet = wks.worksheet(sheet)
    return sheet


def get_df(sheet):
    data = sheet.get_all_values()
    data = np.asarray(data)
    df = DataFrame(data[1:], columns=data[0])
    # ffill sparse to dense index.
    levels = ['level0', 'level1', 'level2']
    left = df[levels].copy()
    left[levels[:2]] = left[levels[:2]].mask(left == '', None).ffill()
    index = MultiIndex.from_frame(left)

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
        return to_datetime(col, errors='coerce')
    elif type_str == 'str' or type_str == 'str:previous_project_id':
        return col.astype(str)
    elif type_str == 'bool':
        return col.replace('', '0').astype(int).astype(bool)
    elif type_str == 'int':
        return to_numeric(col.str.replace(',', ''), errors='coerce', downcast='float')
    elif type_str == 'float':
        return to_numeric(col.str.replace(',', ''), errors='coerce', downcast='float')
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


def load_retro_from_sheets(fname="Forest-Offset-Projects-v0.3"):
    sheet = get_sheet("ifm", fname)

    df, types = get_df(sheet)

    for index, col in df.iteritems():
        type_str = types[index]
        df[index] = cast_col(col, type_str)
    return df
