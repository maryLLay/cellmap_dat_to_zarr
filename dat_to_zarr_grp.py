from fibsem_tools.io import read
import zarr
from pathlib import Path
from numcodecs import GZip

source_path = '/nearline/cellmap/data/aic_desmosome-1/raw/Gemini450-0113_21-01-13_211813_0-0-0.dat'
container_path = 's3://janelia-cosem-dev/test_bucket/test.zarr'

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
    dat_to_zarr(source_path, container_path)