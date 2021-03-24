import zipfile
from itertools import chain
from pathlib import Path

ODPR_PATH = Path(__file__).parents[1] / 'data' / 'raw_opdrs'
OUT_PATH = Path(__file__).parents[1] / 'data' / 'packaged_opdrs'


def get_fnames_to_package(path):
    patterns = ['Initial*.pdf', 'RP-*.pdf']
    return chain(*[list(path.glob(pattern)) for pattern in patterns])  # chain flattens nested lists


def package_dir(opr_id, odpr_path=ODPR_PATH, out_path=OUT_PATH):
    path = Path(odpr_path) / opr_id

    fpaths = get_fnames_to_package(path)

    out_fn = Path(out_path) / f"{opr_id}.zip"
    with zipfile.ZipFile(out_fn, 'w') as zip_f:
        for fpath in fpaths:
            zip_f.write(fpath, fpath.name)


if __name__ == '__main__':

    opr_ids = [p.stem for p in Path(ODPR_PATH).glob('*') if not p.stem.startswith(('.'))]
    for opr_id in opr_ids:
        package_dir(opr_id)
