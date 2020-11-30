import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import statsmodels.formula.api as smf  # looks like this is outdated -- but backward compatibility ftw!

import utils


def build_cp_to_baseline_model(plot=False):
    proj_df = utils.load_retro_from_json('data/projects.json')
    ea_d = proj_df['project']['early_action'].str.startswith('CAR').to_dict()
    non_ea = [proj_id for proj_id, is_ea in ea_d.items() if is_ea == False]

    sub = proj_df.loc[non_ea]
    sub = sub.loc[sub['baseline']['initial_carbon_stock'] > sub['baseline']['common_practice']]
    # TODO: see how baselines deal with ifm_3 -- i suspect some harvest, others dont
    baseline_carbon = (
        sub['baseline']['components']['ifm_1'] + sub['baseline']['components']['ifm_3']
    ) / sub['project']['acreage']
    baseline_carbon.name = 'baseline_carbon'

    for_reg = pd.concat([sub['baseline']['common_practice'], baseline_carbon], axis=1)
    if plot:
        sns.lmplot(
            'common_practice',
            'baseline_carbon',
            data=for_reg,
            scatter_kws={'s': 175, 'color': 'k', 'alpha': 0.5},
            ci=False,
            line_kws={'color': 'k'},
        )
        plt.plot((0, 250), (0, 250), ls='--', lw=3, c='r')
        plt.xlim(0, 250)
        plt.ylim(0, 250)
        # plt.grid()
        plt.xlabel("Project Common Practice\n(MtCO2e acre$^{-1}$)")
        plt.ylabel("Baseline Onsite Carbon\n(MtCO2e acre$^{-1}$)")
    mod = smf.ols('baseline_carbon~common_practice', data=for_reg)
    res = mod.fit()
    return res




def assign_assessment_area(df, lon_var, lat_var, locid_var, shp_path):
    """Joins DataFrame with Taxi Zones shapefile.
    This function takes longitude values provided by `lon_var`, and latitude
    values provided by `lat_var` in DataFrame `df`, and performs a spatial join
    with the NYC taxi_zones shapefile.
    The shapefile is hard coded in, as this function makes a hard assumption of
    latitude and longitude coordinates.

    Only rows where `df.lon_var`, `df.lat_var` are reasonably near New York,
    and `df.locid_var` is set to np.nan are updated.
    Parameters
    ----------
    df : pandas.DataFrame or dask.DataFrame
        DataFrame containing latitudes, longitudes, and location_id columns.
    lon_var : string
        Name of column in `df` containing longitude values. Invalid values
        should be np.nan.
    lat_var : string
        Name of column in `df` containing latitude values. Invalid values
        should be np.nan
    locid_var : string
        Name of series to return.
    """

    # make a copy since we will modify lats and lons
    localdf = df[[lon_var, lat_var]].copy()

    # no nans
    localdf = localdf.dropna(subset=[lon_var, lat_var])

    shape_df = gpd.read_file(shp_path)
    shape_df = shape_df.reset_index()
    shape_df = shape_df.to_crs({'init': 'epsg:4326'})

    try:
        local_gdf = gpd.GeoDataFrame(
            localdf,
            crs={'init': 'epsg:4326'},
            geometry=[Point(xy) for xy in zip(localdf[lon_var], localdf[lat_var])],
        )

        local_gdf = gpd.sjoin(local_gdf, shape_df, how='left', op='within')

        return local_gdf['index'].rename(locid_var)
    except ValueError as ve:
        print(ve)
        print(ve.stacktrace())
        series = localdf[lon_var]
        series = np.nan
        return series
