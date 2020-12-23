import hypothesis.strategies as st
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from hypothesis import given

from carbonplan_retro.fire.project_risk import average_risk, integrated_risk

rs = np.random.seed(0)


def make_burned_area_da(kind):
    t = pd.date_range('2000-01', '2020-12', freq='MS')
    shape = (len(t), 4, 5)

    if kind == 'zeros':
        vals = np.zeros(shape)
    elif kind == 'ones':
        vals = np.ones(shape)
    else:
        vals = np.random.random_sample(shape)
        vals[1, 0, 0] = 1  # at least one point is 1
        vals[:, 0, 1] = 0  # at least one point is always 0

    return xr.DataArray(vals, dims=('time', 'y', 'x'), coords={'time': t})


@pytest.fixture
def region_masks_da():
    shape = (2, 4, 5)
    vals = np.zeros(shape, dtype=bool)
    vals[0, 2:4, 0:2] = True
    vals[1, :2, 3:] = True

    return xr.DataArray(vals, dims=('region', 'y', 'x'))


@given(st.floats(0, 1))
def test_integrated_risk(p):
    risk = integrated_risk(p)
    assert 0 <= risk <= 1


def test_average_risk_random(region_masks_da):
    burned_area_da = make_burned_area_da('random')
    da = average_risk(burned_area_da, region_masks_da)
    assert (da > 0).all()
    assert (da <= 1).all()


def test_average_risk_all_ones(region_masks_da):
    burned_area_da = make_burned_area_da('ones')
    da = average_risk(burned_area_da, region_masks_da)
    assert (da == 1).all()


def test_average_risk_all_zeros(region_masks_da):
    burned_area_da = make_burned_area_da('zeros')
    da = average_risk(burned_area_da, region_masks_da)
    assert (da == 0).all()
