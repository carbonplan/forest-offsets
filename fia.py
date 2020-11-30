import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


def to_geodataframe(df, lat_key='LAT', lon_key='LON'):
    geo_df = gpd.GeoDataFrame(
        df, crs='epsg:4326', geometry=[Point(xy) for xy in zip(df[lon_key], df[lat_key])]
    )
    return geo_df


def load_arb_shapes(postal_code):
    if postal_code == 'ak':
        gdf = gpd.read_file('/home/jovyan/carbonplan/forests/notebooks/ak_assessment_areas/')
    else:
        gdf = pd.read_file('/home/jovyan/carbonplan/forests/notebooks/2015_arb_supersections/')
    return gdf.to_crs('epsg:4326')


def load_omernik(postal_code):
    if postal_code == 'ak':
        omernik = gpd.read_file('/home/jovyan/carbonplan/retro/data/ak_omernik/')
    else:
        raise NotImplementedError('only have Omernik for AK right now')
    return omernik.to_crs('epsg:4326')


def load_ecomap(postal_code):
    if postal_code == 'ak':
        ecomap = gpd.read_file('/home/jovyan/carbonplan/retro/akecoregions-ShapeFile')
        ecomap = ecomap.set_crs('epsg:3338')
        ecomap = ecomap.dropna()
    else:
        raise NotImplementedError('only have ECOMAP for AK right now')

    return ecomap.to_crs('epsg:4326')



def load_fia_data(postal_code, kind=None, filter_data=True):
    """

    """
    if kind not in ['long', 'tree']:
        raise NotImplementedError('kind must be in ["long", "tree"]')

    if kind == 'long':
        df = load_fia_long(postal_code)

    if kind == 'tree':
        df = load_fia_tree(postal_code)

    if filter_data:
        if postal_code == 'ak':
            criteria = (df['INVYR'] > 2000) & (df['INVYR'] < 2013) & (~df['OWNCD'].isin([21, 31, 32]))
        else:
            criteria = (df['INVYR'] > 2000) & (df['INVYR'] < 2013) & (df['OWNCD'] == 46)
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
        'LAT',
        'LON',
        'ELEV',
    ]

    df = pd.read_parquet(fn, columns=usecols)
    df = df.dropna(subset=["LAT", "LON", 'adj_ag_biomass'])

    df['slag_co2e_acre'] = df['adj_ag_biomass'] * (44 / 12) * (1 / 2.47) * 0.5
    df['site_class'] = 'low'
    df.loc[df['SITECLCD'] < 4, 'site_class'] = 'high'

    df['is_private'] = 0
    df.loc[df.OWNCD == 46, 'is_private'] = 1
    df = to_geodataframe(df)

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
            'CONDID',
        ],
    )

    tree_df = tree_df.join(plot_df.set_index(['CN']), on='PLT_CN', how='inner')
    tree_df = tree_df.join(cond_agg, rsuffix='_cond', on=['PLT_CN', 'CONDID'])
    tree_df = to_geodataframe(tree_df)

    return tree_df
