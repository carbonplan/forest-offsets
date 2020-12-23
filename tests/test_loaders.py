import pytest

from carbonplan_retro.load.arb import load_olaf_common_practice, load_olaf_df
from carbonplan_retro.load.assessment_area import load_assessment_areas
from carbonplan_retro.load.fia import fia
from carbonplan_retro.load.geometry import (
    ifm_shapes,
    load_ak_supersection,
    load_arb_shapes,
    load_ecomap,
    load_omernik,
    load_supersection_shapes,
)
from carbonplan_retro.load.issuance import get_arb_id_map, load_issuance_table


def test_load_olaf_common_practice():
    df = load_olaf_common_practice()
    assert len(df)


def test_load_olaf_df():
    df = load_olaf_df()
    assert len(df)


@pytest.mark.parametrize('year', [2011, 2014, 2015])
def test_load_assessment_areas(year):
    df = load_assessment_areas(year)
    assert len(df)


@pytest.mark.parametrize('kind', ['long', 'tree'])
def test_fia(kind):
    postal_code = 'VT'
    gdf = fia(postal_code, kind=kind)
    assert len(gdf)


def test_load_ak_supersection():
    df = load_ak_supersection()
    assert len(df)


@pytest.mark.parametrize('postal_code', ['ak', None])
def test_load_arb_shapes(postal_code):
    df = load_arb_shapes(postal_code)
    assert len(df)


@pytest.mark.parametrize('postal_code', ['ak', None])
def test_load_omernik(postal_code):
    if postal_code == 'ak':
        df = load_omernik(postal_code)
        assert len(df)
    else:
        with pytest.raises(NotImplementedError):
            load_omernik(postal_code)


@pytest.mark.parametrize('postal_code', ['ak', None])
def test_load_ecomap(postal_code):
    df = load_ecomap(postal_code)
    assert len(df)


def test_load_supersection_shapes():
    # TODO: figure out fn parameter
    df = load_supersection_shapes()
    assert len(df)


def test_ifm_shapes():
    # TODO: parameterize
    df = ifm_shapes()
    assert len(df)


def test_load_issuance_table():
    all_projects = load_issuance_table(forest_only=False)
    forest_projects = load_issuance_table(forest_only=True)
    assert len(all_projects) > len(forest_projects)


def test_get_arb_id_map():
    arb_id_map = get_arb_id_map()
    assert isinstance(arb_id_map, dict)
