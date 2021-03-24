import pytest

from carbonplan_forest_offsets.data import cat


@pytest.mark.parametrize("key", list(cat))
def test_load_catalog_entries(key):
    try:
        obj = cat[key].to_dask()
    except NotImplementedError:
        obj = cat[key].read()

    assert obj is not None
