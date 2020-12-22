from carbonplan_retro.data import cat
from carbonplan_retro.load.issuance import load_issuance_table


def test_load_catalog():
    for k in list(cat):
        try:
            cat[k].to_dask()
        except NotImplementedError:
            cat[k].read()


def test_load_issuance_table():
    all_projects = load_issuance_table(forest_only=False)
    forest_projects = load_issuance_table(forest_only=True)
    assert len(all_projects) > len(forest_projects)
