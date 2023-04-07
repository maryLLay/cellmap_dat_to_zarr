from fibsem_tools.io import read
import zarr
from pathlib import Path
from numcodecs import GZip
import dask.bag as db
import os

#source_path = '/nearline/cellmap/data/aic_desmosome-1/raw/Gemini450-0113_21-01-13_211813_0-0-0.dat'
source_path = '/dm11.hhmi.org/projtechres$/laym/Documents/CellMap/jrc_hela2_raw_testData'
#container_path = 's3://janelia-cosem-dev/test_bucket/test.zarr'
container_path = '/dm11.hhmi.org/projtechres$/laym/Documents/CellMap/output_test'

def dat_to_zarr(source_path,
                group_path,
                chunks=(2048, 2048, 1),
                compressor=GZip(level=-1)):
    data = read(source_path)
    name = Path(source_path).stem
    new_array = zarr.open(store=group_path,
                          shape=data.shape,
                          path=name,
                          chunks=chunks,
                          compressor=compressor,
                          dimension_separator="/",
                          dtype=data.dtype)
    new_array[:] = data
    new_array.attrs.update(**dict(data.attrs))
    return new_array.path


if __name__ == '__main__':
    from zarr.storage import FSStore
    store = FSStore(container_path, mode='w')
    group = zarr.group(store=store, overwrite=True)

    #new stuff here- dask doing the task
    dir_list = os.listdir(source_path)
    data = db.read_text(dir_list)
    result = db.map(dat_to_zarr, data, container_path)
    #dat_to_zarr(source_path, container_path)

    #more new stuff here- Client and compute   
    from dask.distributed import Client
    client = Client() #local atm; need cluster address(?)
    result.compute()

