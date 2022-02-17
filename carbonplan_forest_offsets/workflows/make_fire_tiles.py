from prefect import Flow, Parameter
from prefect.core.parameter import DateTimeParameter
from prefect.tasks.shell import ShellTask

from carbonplan_forest_offsets.tasks import nifc

NIFC_BUCKET = 'carbonplan-scratch/offset-fires'
UPLOAD_TO = "carbonplan-scratch/offset-fires/tiles"

build_tiles_from_json = ShellTask(name="transform json to mbtiles")
build_pbf_from_tiles = ShellTask(name='transform mbtiles to pbf')

with Flow("make-fire-tiles") as flow:
    stem = Parameter("stem", default="fire")
    as_of = DateTimeParameter("as_of", required=False)
    tempdir = nifc.make_tile_tempdir()

    nifc_fn = nifc.get_nifc_filename(bucket=NIFC_BUCKET, as_of=as_of)
    nifc_data = nifc.load_nifc_data(nifc_fn)
    nifc_json = nifc.get_fires_json(nifc_data)

    json_fn = nifc.write_fire_json(nifc_json, tempdir)
    tippecanoe_cmd = nifc.build_tippecanoe_cmd(json_fn, tempdir, stem)
    tiles = build_tiles_from_json(command=tippecanoe_cmd)

    # must specify upstream, otherwise race condition
    pbf_cmd = nifc.build_pbf_cmd(tempdir, stem, upstream_tasks=[tiles])

    pbf = build_pbf_from_tiles(command=pbf_cmd)

    nifc.upload_tiles(tempdir, stem, UPLOAD_TO, upstream_tasks=[pbf])
