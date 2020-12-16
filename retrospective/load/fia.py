import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from .geometry import load_arb_shapes, load_ecomap, load_omernik


def to_geodataframe(df, lat_key='LAT', lon_key='LON'):
    geo_df = gpd.GeoDataFrame(
        df, crs='epsg:4326', geometry=[Point(xy) for xy in zip(df[lon_key], df[lat_key])]
    )
    return geo_df


def fia(postal_code, kind=None, filter_data=True):
    """
    postal_code: two letter state abbr
    kind: return processed `long` data from carbonplan_forests or raw plot/cond/tree data
    filter_data: filter data to ~approximate data used in CARB 2015 protocol CP calculations.
    """
    if kind not in ['long', 'tree']:
        raise NotImplementedError('kind must be in ["long", "tree"]')

    if kind == 'long':
        df = load_fia_long(postal_code)

    if kind == 'tree':
        df = load_fia_tree(postal_code)

    if filter_data:
        if postal_code == 'ak':
            criteria = (
                (df['inventory_year'] > 2000)
                & (df['inventory_year'] < 2013)
                & (~df['owner'].isin([21, 31, 32]))
            )
        else:
            criteria = (
                (df['inventory_year'] > 2000) & (df['inventory_year'] < 2013) & (df['owner'] == 46)
            )
        df = df[criteria]

    # assign all regions we might be interested in!
    # this is slower for tree bc we assign EACH tree as opposed to each plot...but yolo.
    arb_shapes = load_arb_shapes(postal_code)
    df = gpd.sjoin(df, arb_shapes, how='left', op='within', rsuffix='supersection')
    if postal_code == 'ak':
        omernik = load_omernik(postal_code)
        df = gpd.sjoin(
            df, omernik[['US_L3CODE', 'geometry']], how='left', op='within', rsuffix='omernik'
        )

        ecomap = load_ecomap(postal_code)
        df = gpd.sjoin(
            df, ecomap[['COMMONER', 'geometry']], how='left', op='within', rsuffix='ecomap'
        )

    return df


def load_fia_long(postal_code):

    fn = f'gs://carbonplan-data/processed/fia-states/long/{postal_code.lower()}.parquet'

    usecols = [
        'adj_ag_biomass',
        'OWNCD',
        'CONDID',
        'STDAGE',
        'SITECLCD',
        'FORTYPCD',
        'FLDTYPCD',
        'CONDPROP_UNADJ',
        'COND_STATUS_CD',
        'SLOPE',
        'ASPECT',
        'INVYR',
        'MEASYEAR',
        'LAT',
        'LON',
        'ELEV',
    ]

    df = pd.read_parquet(fn, columns=usecols)
    df = df.dropna(subset=["LAT", "LON", 'adj_ag_biomass'])

    df['slag_co2e_acre'] = df['adj_ag_biomass'] * (44 / 12) * (1 / 2.47) * 0.5
    df['site_class'] = 'low'
    df.loc[
        df['SITECLCD'] < 4, 'site_class'
    ] = 'high'  # NB this criteria changes between 2014 & 2015 CARB FOP.

    df = to_geodataframe(df)

    rename_d = {
        'FORTYPCD': 'forest_type',
        'FLDTYPCD': 'field_type',
        'MEASYEAR': 'year',
        'INVYR': 'inventory_year',
        "ELEV": 'elevation',
        "OWNCD": 'owner',
    }
    df = df.rename(columns=rename_d)

    df.columns = [column.lower() for column in df.columns]  # send rest of columns to lower case

    return df


def load_fia_tree(postal_code):
    cond_df = pd.read_parquet(
        f'gs://carbonplan-data/raw/fia-states/cond_{postal_code.lower()}.parquet',
        columns=['CN', 'PLT_CN', 'CONDID', 'OWNCD', 'FORTYPCD', 'FLDTYPCD'],
    )
    cond_agg = cond_df.groupby(['PLT_CN', 'CONDID']).max()

    plot_df = pd.read_parquet(
        f'gs://carbonplan-data/raw/fia-states/plot_{postal_code}.parquet',
        columns=['CN', 'LAT', 'LON', 'ELEV', 'INVYR'],
    )

    tree_df = pd.read_parquet(
        f'gs://carbonplan-data/raw/fia-states/tree_{postal_code.lower()}.parquet',
        columns=[
            'CN',
            'PLT_CN',
            'SPCD',
            'DIA',
            'CONDID',
        ],
    )

    tree_df = tree_df.join(plot_df.set_index(['CN']), on='PLT_CN', how='inner')
    tree_df = tree_df.join(cond_agg, rsuffix='_cond', on=['PLT_CN', 'CONDID'])
    tree_df = to_geodataframe(tree_df)

    return tree_df
