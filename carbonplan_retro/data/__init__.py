import os
import pathlib

import fsspec
from intake import open_catalog

cat_dir = pathlib.Path(__file__)
cat_file = str(cat_dir.parent / "catalog.yaml")
cat = open_catalog(cat_file)


def get_temp_bucket():
    prefix = 'az://carbonplan-scratch'
    kwargs = {'account_name': 'carbonplan', 'account_key': os.environ.get('BLOB_ACCOUNT_KEY', None)}
    return prefix, kwargs


def get_retro_bucket():
    prefix = 'az://carbonplan-retro'
    kwargs = {'account_name': 'carbonplan', 'account_key': os.environ.get('BLOB_ACCOUNT_KEY', None)}
    return prefix, kwargs


def get_filesystem():
    fs = fsspec.get_filesystem_class('az')(
        account_name='carbonplan', account_key=os.environ['BLOB_ACCOUNT_KEY']
    )
    return fs
