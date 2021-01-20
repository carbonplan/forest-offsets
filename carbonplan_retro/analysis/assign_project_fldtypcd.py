import math

import dask.dataframe as dd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction import DictVectorizer
from sklearn.model_selection import train_test_split

from carbonplan_retro.data import cat


def fractional_basal_area_by_species(data):
    """For group of trees, calcuate the fraction total basal area represented by each species"""
    # cast to str so can store sparsely :)
    fractional_ba = (
        data.groupby(data["SPCD"].astype(str)).unadj_basal_area.sum() / data.unadj_basal_area.sum()
    ).round(4)
    return fractional_ba.to_dict()


def load_cond_data(postal_codes):
    cond_cols = ['CN', 'PLT_CN', 'CONDID', 'OWNCD', 'FORTYPCD', 'FLDTYPCD']

    plot_cols = ['CN', 'LAT', 'LON', 'ELEV', 'INVYR']
    cond_ddf = dd.concat(
        [
            cat.fia(postal_code=postal_code.lower(), table='cond', columns=cond_cols).to_dask()
            for postal_code in postal_codes
        ]
    )
    plot_ddf = dd.concat(
        [
            cat.fia(postal_code=postal_code.lower(), table='plot', columns=plot_cols).to_dask()
            for postal_code in postal_codes
        ]
    )
    conds = cond_ddf.compute()
    plots = plot_ddf.compute()

    return conds.join(plots.set_index('CN'), on=['PLT_CN'])


def load_cond_classification_data(postal_codes):
    cond_cols = ['CN', 'PLT_CN', 'CONDID', 'OWNCD', 'FORTYPCD', 'FLDTYPCD']

    plot_cols = ['CN', 'LAT', 'LON', 'ELEV', 'INVYR']
    cond_ddf = dd.concat(
        [
            cat.fia(postal_code=postal_code.lower(), table='cond', columns=cond_cols).to_dask()
            for postal_code in postal_codes
        ]
    )
    plot_ddf = dd.concat(
        [
            cat.fia(postal_code=postal_code.lower(), table='plot', columns=plot_cols).to_dask()
            for postal_code in postal_codes
        ]
    )
    conds = cond_ddf.compute()
    plots = plot_ddf.compute()

    return conds.join(plots.set_index('CN'), on=['PLT_CN'])


def load_tree_classificaiton_data(postal_codes):
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
        ]
    )
    trees = trees[trees['STATUSCD'] == 1]  # only looking at live trees
    trees['unadj_basal_area'] = math.pi * (trees['DIA'] / (2 * 12)) ** 2 * trees['TPA_UNADJ']
    features = trees.groupby(['PLT_CN', 'CONDID']).apply(
        fractional_basal_area_by_species, meta=('fraction_species', 'f4')
    )
    features = features.compute()
    return features


def load_classification_data(postal_codes, target_var='FLDTYPCD'):
    tree_features = load_tree_classificaiton_data(postal_codes)
    conds = load_cond_classification_data(postal_codes)
    data = conds.join(tree_features, on=['PLT_CN', 'CONDID']).dropna(
        subset=[target_var, 'fraction_species']
    )
    data = data.loc[
        (data['FORTYPCD'] != 999)
    ]  # dont include non-stocked because projects cant unstocked!

    data = data.loc[(data[target_var] < 962)]  # exclude wastebasket forest types
    target_counts = data['targets'].value_counts()
    valid_target_classes = target_counts[target_counts > 30].index.unique().tolist()
    data = data[data[target_var].isin(valid_target_classes)]

    vec = DictVectorizer()
    X = vec.fit_transform(
        data["fraction_species"].values
    ).toarray()  # .toarray() explodes the sparse array returned from DictVectorizer() out into a dense array
    y = data[target_var].values

    return {'features': X, 'targets': y, 'dictvectorizer': vec}


def train_classifier(X, y, n_estimators=10_000):
    # params from grid-search -- in general lots and lots of shallow-ish trees seem to work well.
    X_train, X_calib, y_train, y_calib = train_test_split(
        X, y, test_size=0.25, random_state=2020, stratify=''
    )
    clf = RandomForestClassifier(
        random_state=2020,
        n_estimators=n_estimators,
        min_samples_split=5,
        n_jobs=-1,
        max_depth=15,
        min_samples_leaf=2,
    )
    clf.fit(X_train, y_train)

    calibrated_clf = CalibratedClassifierCV(base_estimator=clf, cv='prefit')
    calibrated_clf.fit(X_calib, y_calib)
    return calibrated_clf
