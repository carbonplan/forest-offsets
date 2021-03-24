import math

import dask
import dask.dataframe as dd
import geopandas
from sklearn.feature_extraction import DictVectorizer

from ..data import cat
from ..utils import to_geodataframe


def fractional_basal_area_by_species(data):
    """For group of trees, calculate the fraction total basal area represented by each species"""
    # cast to str so can store sparsely :)
    fractional_ba = (
        (
            data.groupby(data["SPCD"].astype(str)).unadj_basal_area.sum()
            / data.unadj_basal_area.sum()
        )
        .round(4)
        .dropna()
    )
    return fractional_ba.to_dict()


def load_cond_classification_data(postal_codes):
    cond_cols = ['CN', 'PLT_CN', 'CONDID', 'OWNCD', 'FORTYPCD', 'FLDTYPCD', 'COND_STATUS_CD']

    plot_cols = ['CN', 'LAT', 'LON', 'ELEV', 'INVYR']
    cond_ddf = dd.concat(
        [
            cat.fia(postal_code=postal_code.lower(), table='cond', columns=cond_cols).to_dask()
            for postal_code in postal_codes
        ],
        ignore_index=True,
    )
    cond_ddf = cond_ddf[cond_ddf['COND_STATUS_CD'] == 1]  # forest conditions only
    plot_ddf = dd.concat(
        [
            cat.fia(postal_code=postal_code.lower(), table='plot', columns=plot_cols).to_dask()
            for postal_code in postal_codes
        ],
        ignore_index=True,
    )

    conds, plots = dask.compute(cond_ddf, plot_ddf)

    conds = conds.join(plots.set_index('CN'), on=['PLT_CN']).reset_index()
    return conds


def load_tree_classification_data(postal_codes):
    tree_cols = [
        'CN',
        'PLT_CN',
        'CONDID',
        'STATUSCD',
        'TPA_UNADJ',
        'SPCD',
        'DIA',
    ]

    trees = dd.concat(
        [
            cat.fia(postal_code=postal_code, table='tree', columns=tree_cols).to_dask()
            for postal_code in postal_codes
        ],
        ignore_index=True,
    )
    trees = trees[trees['STATUSCD'] == 1]  # only looking at live trees
    trees['unadj_basal_area'] = math.pi * (trees['DIA'] / (2 * 12)) ** 2 * trees['TPA_UNADJ']
    features = trees.groupby(['PLT_CN', 'CONDID']).apply(
        fractional_basal_area_by_species, meta=('fraction_species', 'f4')
    )
    features = features.compute()
    return features


def load_classification_data(postal_codes, target_var='FORTYPCD', aoi=None):
    tree_features = load_tree_classification_data(postal_codes)
    conds = load_cond_classification_data(postal_codes)
    conds = conds[(conds['INVYR'] >= 2002) & (conds['INVYR'] < 2013)]
    if aoi:
        conds = to_geodataframe(conds)
        conds = geopandas.clip(conds, aoi)

    data = conds.join(tree_features, on=['PLT_CN', 'CONDID']).dropna(
        subset=[target_var, 'fraction_species']
    )
    data = data.loc[
        (data['FORTYPCD'] != 999)
    ]  # dont include non-stocked because projects cant be unstocked!

    # exclude wastebasket forest types, filtering on both for and fld typ
    data = data.loc[(data['FORTYPCD'] < 962) & (data['FLDTYPCD'] < 962)]

    # only include fortypcs with at least 30 conds
    target_counts = data[target_var].value_counts()
    valid_target_classes = target_counts[target_counts > 30].index.unique().tolist()

    data = data[data[target_var].isin(valid_target_classes)]
    data = data.dropna(subset=[target_var, 'fraction_species'])
    vec = DictVectorizer()
    X = vec.fit_transform(data["fraction_species"].values)
    y = data[target_var].values

    return {'features': X, 'targets': y, 'dictvectorizer': vec}
