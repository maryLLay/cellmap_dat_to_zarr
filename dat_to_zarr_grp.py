from fibsem_tools.io import read
import zarr
from numpy import shape as shp

path_to_data = r'C:\Users\laym\Documents\CellMap\jrc_hela2_raw_testData\Gemini500-7029_17-06-21_200906_0-0-0.dat'
path_to_group = r'C:\Users\laym\Documents\CellMap\Test'

def create_group(group_path):
    zarr.group(store=group_path, overwrite=True)
    return group_path

def create_array(data_path, group_path): #This needs to be run for every .dat in a given directory
    data = read(data_path)
    shape = shp(data) #This needs to be improved
    name = data_path[-16:-4]  #This needs to be improved
    new_array = zarr.open(store=group_path, shape=shape, path=name, chunks=(10, 10, 1))
    new_array[:] = data #There's probably a shorter way to combo this with the step above
    return data_path

create_array(path_to_data, path_to_group)
