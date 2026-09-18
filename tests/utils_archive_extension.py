import os
import tarfile

import numpy as np
import xarray as xr
import zarr

print(zarr.__version__)
print(xr.__version__)
print(np.__version__)

# # Testing zarr stores with sample data

times = np.arange('2023-01-01', '2023-01-03', dtype='datetime64[D]')
lats = [34, 35, 36]
lons = [-118, -117, -116]
variableDict= {
    'temperature' : {
            'values' : np.random.rand(2, 3, 3) * 30 + 273.15, # (time, lat, lon)
            'units' : "K"
            },
    'precipitation' : {
            'values' : np.random.rand(2, 3, 3),  # (time, lat, lon)
            'units' : "mm/day"
            }
}
zarrStorePrefix='testStore'
zarrStoreList = []
tarStoreList = []
list_of_vars=[]

def createZarrStoreWithSingleVariable( varName, index ):
    # Create the Dataset
    ds = xr.Dataset(
        data_vars={
            f"{varName}": (
                            ("time", "lat", "lon"),
                            variableDict[varName]['values'],
                            {"units": variableDict[varName]['units']}),
        },
        coords={
            "time": times,
            "lat": lats,
            "lon": lons
        },
        attrs={"description": "Sample weather data"}
    )
    #Convert and save to zarr store with 'index' in the name.
    zarrStoreName = f'./data/{zarrStorePrefix}{index}Dev.zarr'
    ds.to_zarr( f'{zarrStoreName}', mode='w', zarr_format=3 )
    zarrStoreList.append(zarrStoreName)


def scan_dir( dirName ):
    with os.scandir( dirName ) as it:
        for entry in it:
            if entry.is_file():
                list_of_vars.append( entry.path )
            elif entry.is_dir():
                scan_dir( entry.path )


def createTarStoreFromZarrStore(zarrStoreName):
    zarrBaseName=os.path.basename(zarrStoreName).split('.')[0]
    tarFileName=f'./data/{zarrBaseName}.tar'
    list_of_vars.clear()

    scan_dir( zarrStoreName )

    try:
        with tarfile.open(tarFileName, "w") as tar:
            for name in list_of_vars:
                #print(f"Adding file {name} to {tarFileName}\n")
                tar.add( name, arcname=name.replace( zarrStoreName + os.path.sep, '' ) )
        tar.close()
        tarStoreList.append(tarFileName)
    except Exception as e:
        print(f"Exception occured: {e}")

# Create Zarr store
count = 1
for variable in variableDict.keys():
    createZarrStoreWithSingleVariable( variable, count )
    count += 1

# Convert zarr store to Tarstore
for zarrStore in zarrStoreList:
    createTarStoreFromZarrStore( zarrStore )
