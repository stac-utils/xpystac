# Standard modules
import os
import sys

#Non-standard modules

try:
    import xarray as xr
except Exception:
    print( sys.exc_info() )
    print( f"Module 'xarray' import error in {__file__}" )

try:
    import zarr
except Exception:
    print( sys.exc_info() )
    print( f"Module 'zarr' import error in {__file__}" )

try:
    pass
except Exception:
    print( sys.exc_info() )
    print( f"Module 'dask' import error in {__file__}" )


try:
    import xpystac
except Exception:
    print( sys.exc_info() )
    print( f"Module 'xpystac' import error in {__file__}" )

try:
    import pystac
except Exception:
    print( sys.exc_info() )
    print( f"Module 'pystac' import error in {__file__}" )


try:
    from pystac.extensions.archive import ArchiveExtension
except  Exception:
    print( sys.exc_info() )
    print( f"Module 'pystac.extensions.archive.ArchiveExtension' \
            import error in {__file__}" )


try:
    pass
except Exception:
    print( sys.exc_info() )
    print( f"Module 'utils_archive_extension' import error in {__file__}" )

try:
    from utils_archive_extension import (
        createTarStoreFromZarrStore,
        createZarrStoreWithSingleVariable,
        variableDict,
        zarrStoreList,
    )

except Exception:
    print( sys.exc_info() )
    print( f"Module 'utils_archive_extension.(variableDict, zarrStoreList, \
                createZarrStoreWithSingleVariable, createTarStoreFromZarrStore' \
                import error in {__file__}" )

print(zarr.__version__)
print(xr.__version__)
print(pystac.__version__)
print(xpystac.__version__)

engineList = xr.backends.list_engines().keys()
assert 'stac' in engineList

# Create Zarr store
count = 1
for variable in variableDict.keys():
    createZarrStoreWithSingleVariable( variable, count )
    count += 1

# Convert zarr store to Tarstore
for zarrStore in zarrStoreList:
    createTarStoreFromZarrStore( zarrStore )


tarStorePathList = [ './data/testStore1Dev.tar', './data/testStore2Dev.tar' ]

for testStore in tarStorePathList:
    assert  os.path.exists(testStore)

# # Single Tar store read with zarr 'TarStore'


with zarr.storage.TarStore( tarStorePathList[0], mode='r' ) as store:
    ds = xr.open_zarr(store).compute()
assert ds


# # Multiple Tar stores with zarr 'TarStore'

tarStoreList = [ zarr.storage.TarStore( storePath, mode='r')
                    for storePath in tarStorePathList ]
dsFull = xr.open_mfdataset( tarStoreList, engine = 'zarr' )
assert dsFull
#dsFull.compute()


# # Accessing assets from STAC catalog and creating 'archiveextension' based assets

ngc4008_collection=pystac.Collection.from_file("https://wwestac.cloud.dkrz.de/stac-fastapi-es/collections/ngc4008")
ngc4008_collection.to_dict()
items=pystac.ItemCollection.from_file("https://wwestac.cloud.dkrz.de/stac-fastapi-es/collections/ngc4008/items")
item_count = len(items)

assert item_count > 0

item = items[1]
item.id
item.to_dict()
assert item.assets is not None

item.assets['tape1'] = {'href': tarStorePathList[0],
   'type': 'application/x-tar',
   'archive:format': 'application/x-tar',
   'archive:href': tarStorePathList[0],
   'archive:type': 'application/vnd+zarr'}


item.assets['tape2'] = {'href': tarStorePathList[1],
   'type': 'application/x-tar',
   'archive:format': 'application/x-tar',
   'archive:href': tarStorePathList[1],
   'archive:type': 'application/vnd+zarr'}

item.assets

ArchiveExtension.add_to(item)

assert ArchiveExtension.get_schema_uri() in item.stac_extensions


# # Accessing single STAC asset with 'archiveextension' using xpystac and
# reading with zarr 'TarStore'
tape_asset=item.assets['tape1']
tape_asset
ds_tar=xr.open_dataset(pystac.asset.Asset.from_dict(tape_asset),engine="stac")
assert ds_tar

# # Accessing multiple STAC assets with 'archiveextension' using xpystac and
# reading with zarr 'TarStore'

tape_assets=[ item.assets['tape1'], item.assets['tape2'] ]
tape_assets

tape_assetList = [pystac.asset.Asset.from_dict(tape_asset)
                    for tape_asset in tape_assets]
ds_tarList=xr.open_dataset( tape_assetList, engine="stac" )
assert ds_tarList

assert len ( ds_tarList.variables.keys() ) > 0

for var in variableDict.keys():
    assert var in ds_tarList.variables.keys()
