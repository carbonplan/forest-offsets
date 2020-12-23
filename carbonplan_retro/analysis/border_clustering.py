import random

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point


def generate_random_points_in_polygon(gdf: gpd.GeoDataFrame):
    """Genterate random number of points within a polygon
    spatial sampling, in a truly uniform manner, is a really tough problem.
    Likely has some biases -- but think we can solve numerically (lots of samples!!!)"""
    # TODO: error if gpd has units != meters
    # TODO: error if len > 1?
    if not gdf.crs.is_projected:
        raise NotImplementedError("Only gonna work for projected (meter) crs")
    points_per_area = int(
        gdf.area.item() * (1 / 10000000)
    )  # make so sample max 33k for a single SS; median 5k.
    n_points = max(points_per_area, 2500)  # need at least 2500 points, no matter what

    # Buffer by 10km and take convex hull -- this is to ensure we sample sufficiently along the border
    bounds_d = gdf.buffer(10000).convex_hull.bounds.iloc[0].to_dict()

    points = []
    while len(points) < n_points:
        point = Point(
            random.uniform(bounds_d["minx"], bounds_d["maxx"]),
            random.uniform(bounds_d["miny"], bounds_d["maxy"]),
        )
        if point.within(gdf.geometry.item()):
            points.append(point)

    points = gpd.GeoSeries(points, crs=gdf.crs)
    points_df = gpd.GeoDataFrame({"key": [k for k in np.arange(n_points)]}, geometry=points)

    return points_df


def median_distance(geom: gpd.GeoDataFrame, points):
    """This quickly got difficult..."""
    exploded = geom.explode()
    records = []
    for row in exploded.itertuples():
        exterior = row.geometry.exterior
        distances = [
            point.distance(exterior)
            for point in points.geometry.values
            if point.within(row.geometry)
        ]
        # store.append(distances)
        record = {
            "quantiles": pd.Series(distances).quantile(
                q=[0.01, 0.025, 0.25, 0.5, 0.75, 0.975, 0.99]
            ),
            "area": row.geometry.area,
        }
        records.append(record)

    # return store
    total_area = sum([record["area"] for record in records])
    nan_areas = [record["area"] for record in records if any(np.isnan(record["quantiles"].values))]
    if any(nan_areas):
        # check if bad missing and raise
        unsampled_area = sum(nan_areas) / total_area
        if unsampled_area > 0.01:
            raise
    return [record["quantiles"] * (record["area"] / total_area) for record in records]


def get_distance_to_border(border, points):
    distances = [point.distance(border) for point in points.geometry.values]
    return distances
