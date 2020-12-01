from glob import glob
import json

import subprocess
import geopandas as gpd

from utils import get_arb_id_map

def process_raw_file(fname):
    """ split raw json batch downloaded from ARB ArcGIS server

    """
    arb_id_to_proj_id = get_arb_id_map()
    gdf = gpd.read_file(fname)

    for i in range(1, len(gdf) + 1): # head/tail are not zero-indexed. see comment below.
        """
        This is crazy. Don't judge.
        But gdf.iloc[i], gdf.iterrows() etc return pandas.Series, *not* geopandas.GeoSeries
        Calling .to_json on base pd.Series results in maximum recursion error.
        Recasting Series to GeoSeries isnt allowed and casting to GeoDataFrame raises int64 encoding errors. 
        So I just rolled this head/tail solution that preserves type and works...
        """
        single_row = gdf.head(i).tail(1)
        arb_id = single_row['arb_id'].item()
        proj_id = arb_id_to_proj_id.get(arb_id)
        fname = f'/Users/darryl/proj/carbonplan/retro/data/geometry/projects/{proj_id}.json'
        single_row.to_file(fname, driver='GeoJSON')
        cmd = f"mapshaper {fname} -clean -simplify 0.5 -o force {fname}"
        print(cmd)
        subprocess.call(cmd, shell=1)



if __name__ == '__main__':
    fnames = glob('../data/geometry/projects/raw/*')
    for fname in fnames:
        print(f"processing {fname}...")
        process_raw_file(fname)


