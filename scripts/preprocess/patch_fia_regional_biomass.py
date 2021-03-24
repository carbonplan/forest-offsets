import os
import shutil
from pathlib import Path

import pandas as pd

DIRNAME = Path(__file__).parents[2] / 'data' / 'fia'  # where FIA data stored


def patch_tree_with_regional_biomass(postal_code, input_dir=DIRNAME):
    '''modify FIA `TREE` table to contain regional biomass values -- needed for PNW work unit'''
    tree_path = Path(input_dir) / f"{postal_code}_TREE.csv"
    regional_path = Path(input_dir) / f"{postal_code}_TREE_REGIONAL_BIOMASS.csv"
    if not ((tree_path.exists()) & (regional_path.exists())):
        raise FileNotFoundError('Patching biomass requires TREE_REGIONAL_BIOMASS and TREE tables')

    backup_path = input_dir / 'backup' / tree_path.name
    os.makedirs(backup_path.parent, exist_ok=True)

    if backup_path.exists():
        shutil.copy(backup_path, tree_path)
    else:
        shutil.copy(tree_path, backup_path)

    tree = pd.read_csv(tree_path)

    regional_tree = pd.read_csv(regional_path, usecols=['TRE_CN', 'REGIONAL_DRYBIOT'])

    tree['DRYBIO_TOP'] = 0
    tree['DRYBIO_SAPLING'] = 0
    tree['DRYBIO_WDLD_SPP'] = 0
    tree[
        'DRYBIO_BOLE'
    ] = 0  # zero out just in case there are gaps between TREE and REGIONAL_BIOMASS

    tree['DRYBIO_BOLE'] = tree['CN'].map(
        regional_tree.set_index('TRE_CN')['REGIONAL_DRYBIOT'].to_dict()
    )

    tree.to_csv(tree_path, index=False)


if __name__ == '__main__':

    pnw = ['CA', 'OR', 'WA', 'AK']
    [patch_tree_with_regional_biomass(postal_code) for postal_code in pnw]
