import os
import shutil
import zipfile
from itertools import product
from pathlib import Path

import geopandas

from carbonplan_forest_offsets.data import cat, get_filesystem

TARGET = Path.home() / 'forest-offset-archive'
VERSION = 'v1.0'


def write_inputs():
    '''package external dependencies of forest-offset-paper'''
    write_raw_fia()
    write_fia_long()
    write_prism()
    write_ancillary()


def write_raw_fia():
    """data underlying rFIA and classifier

    N.B. we pulled the FIA data from csv independent of the pull for rFIA meaning small differences might exist at the
    tree level (e.g., revisions in diameter). Rather than package twice, we just include the freshest pull here.
    """
    print("writing raw fia tables -- this takes several minutes")
    src_dir = Path.home() / 'rfia' / 'data'
    fnames = src_dir.glob('*.csv')

    dst = TARGET / 'inputs' / 'fia'
    dst.mkdir(parents=True, exist_ok=True)

    dst_fn = dst / 'raw_fia.zip'

    with zipfile.ZipFile(dst_fn, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for fname in fnames:
            z.write(fname, fname.name)


def write_fia_long():
    '''fia-long from `carbonplan_forests` underlies the spatial arbitrage maps'''
    postal_codes = ['or', 'ca']

    dst = Path(f"{TARGET}/inputs/fia/fia-long")
    dst.mkdir(parents=True, exist_ok=True)

    for postal_code in postal_codes:
        df = cat.fia_long(postal_code=postal_code).read()
        dst_fn = dst / f'{postal_code}.csv'
        df.to_csv(dst_fn, index=False)


def write_prism():
    print('writing prism')
    var_names = ['tmean', 'ppt']
    regions = ['conus', 'ak']

    dst = TARGET / "inputs" / "prism"
    dst.mkdir(parents=True, exist_ok=True)

    for var_name, region in product(var_names, regions):
        da = cat.prism(var=var_name, region=region).read()
        dst_fn = dst / f"{region}_{var_name}.nc"
        da.to_netcdf(dst_fn)


def write_ancillary():
    print('writing ancillary files')
    dst = TARGET / 'inputs' / 'ancillary'
    dst.mkdir(parents=True, exist_ok=True)

    fs = get_filesystem()

    fs.get(
        'carbonplan-retro/ancillary/arb_fortypcds.json',
        str(dst / 'assessment_area_forest_type_codes.json'),
    )

    fs.get('carbonplan-data/raw/ecoregions/supersections.geojson', str(dst / 'supersections.json'))

    ecomap_sections = geopandas.read_file(
        "https://data.fs.usda.gov/geodata/edw/edw_resources/shp/S_USA.EcomapSections.zip"
    ).to_crs("epsg:4326")
    with open(dst / 'ecomap_sections.json', 'w') as f:
        f.write(ecomap_sections.to_json())

    fs.get(
        'carbonplan-retro/ancillary/ne_110m_admin_1_states_provinces.json', str(dst / 'states.json')
    )


def write_intermediates():
    '''package intermediate outputs of forest-offset-paper'''

    print('writing intermediate files')
    dst = TARGET / 'intermediate'
    dst.mkdir(parents=True, exist_ok=True)

    fs = get_filesystem()

    rfia_dst = dst / 'rfia' / 'assessment_areas'
    rfia_dst.mkdir(parents=True, exist_ok=True)
    fs.get(
        'carbonplan-retro/rfia_all/*.csv', str(rfia_dst) + '/'
    )  # append trailing slash to put all csv in dir

    # TODO: ecoseciton analyses
    rfia_ecosection_dst = dst / 'rfia'

    fs.get(
        'carbonplan-retro/results/297_by_ecosection.csv',
        str(rfia_ecosection_dst / '297_by_ecosection.csv'),
    )

    classify_dst = dst / 'classification'
    classify_dst.mkdir(parents=True, exist_ok=True)

    fs.get(
        'carbonplan-scratch/radius_neighbor_params.json',
        str(classify_dst / 'radius_neighbor_params.json'),
    )
    fs.get('carbonplan-retro/classifications.json', str(classify_dst / 'classifications.json'))


def write_results():
    '''package data underlying figures of forest-offset-paper'''
    print('writing results files')
    dst = TARGET / 'results'
    dst.mkdir(parents=True, exist_ok=True)

    fs = get_filesystem()

    fs.get('carbonplan-retro/results/reclassification-crediting-error.json', str(dst) + '/')
    fs.get('carbonplan-retro/arbitrage/prism-supersections/79.json', str(dst) + '/')
    fs.get(
        'carbonplan-retro/results/southern_cascades_mixed_conifer_by_ecosection.json',
        str(dst) + '/',
    )
    fs.get('carbonplan-retro/results/crediting-verification.json', str(dst) + '/')
    fs.get('carbonplan-retro/results/common-practice-verification.json', str(dst) + '/')
    fs.get('carbonplan-retro/reclassification/classifier_fscores.json', str(dst) + '/')
    fs.get('carbonplan-retro/results/issuance_stats.json', str(dst) + '/')
    fs.get('carbonplan-retro/results/reclassification-labels.json', str(dst) + '/')


def zip_archive():

    shutil.make_archive(
        TARGET,
        'zip',
        os.path.expanduser('~'),
        'forest-offset-archive',
    )


def push_archive_to_scratch():
    fs = get_filesystem()

    fs.put_file(
        os.path.expanduser('~/forest-offset-archive.zip'),
        'carbonplan-scratch/forest-offset-archive.zip',
        overwrite=True,
    )


def main():
    write_inputs()
    write_intermediates()
    write_results()

    zip_archive()
    push_archive_to_scratch()


if __name__ == '__main__':
    main()
