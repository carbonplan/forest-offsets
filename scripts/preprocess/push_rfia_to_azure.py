import os

import fsspec

if __name__ == '__main__':
    fs = fsspec.get_filesystem_class('az')(
        account_name='carbonplan', account_key=os.environ['BLOB_ACCOUNT_KEY']
    )
    fs.put(
        '/home/jovyan/rfia/processed_data/no_buffer_biomass/',
        'carbonplan-retro/rfia',
        recursive=True,
        overwrite=True,
    )
    fs.put(
        '/home/jovyan/rfia/processed_data/no_buffer_biomass_all/',
        'carbonplan-retro/rfia_all',
        recursive=True,
        overwrite=True,
    )
