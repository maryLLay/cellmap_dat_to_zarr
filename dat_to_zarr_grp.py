from pathlib import Path
from fibsem_tools.io import read
import zarr
from numcodecs import GZip
import dask.bag as db
import os


source_path = r'\\dm11.hhmi.org\projtechres$\laym\Documents\CellMap\jrc_hela2_raw_testData'


container_path = r'\\dm11.hhmi.org\projtechres$\laym\Documents\CellMap\output_test'

def dat_to_zarr(parent_path,
                group_path,
                chunks=(2048, 2048, 1),
                compressor=GZip(level=-1)):
    dat = read(parent_path)
    name = Path(parent_path).stem
    new_array = zarr.open(store=group_path,
                          shape=dat.shape,
                          path=name,
                          chunks=chunks,
                          compressor=compressor,
                          dimension_separator="/",
                          dtype=dat.dtype)
    new_array[:] = dat
    new_array.attrs.update(**dict(dat.attrs))
    return new_array.path


if __name__ == '__main__':
    from zarr.storage import FSStore
    store = FSStore(container_path, mode='w')
    group = zarr.group(store=store, overwrite=True)

    #new stuff here- dask doing the task
    dir_list = [(source_path + '/' + x) for x in os.listdir(source_path)]
    data = db.from_sequence(dir_list)
    data.compute()
    result = db.map(dat_to_zarr, data, container_path)

    #more new stuff here- Client and compute   
    from dask.distributed import Client
    client = Client() #local atm; need cluster address(?)
    result.compute()

