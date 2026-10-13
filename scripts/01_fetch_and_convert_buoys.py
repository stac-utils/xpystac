"""Fetch the 7 classic NERACOOS wave buoys from ERDDAP and save as real NetCDF4.

ERDDAP's tabledap `.nc` response is classic NetCDF-3, not HDF5/NetCDF4 — and
virtualizarr's HDFParser requires HDF5 — so this re-saves each dataset locally
with format="NETCDF4" after fetching. Idempotent: skips a buoy whose local
file already exists and opens as valid HDF5.

Run with (see scripts/README.md):
    uv run --with erddapy --with xarray --with h5netcdf --with netCDF4 \
        python scripts/01_fetch_and_convert_buoys.py
"""

import sys

import h5py
import xarray as xr
from erddapy import ERDDAP
from fixtures_common import DATASET_IDS, ERDDAP_SERVER, ERDDAP_VARIABLES, netcdf_path

HDF5_MAGIC = b"\x89HDF"


def already_fetched(path) -> bool:
    if not path.exists():
        return False
    try:
        with h5py.File(path, "r"):
            return True
    except Exception:
        return False


def fetch_buoy(buoy: str, dataset_id: str) -> xr.Dataset:
    e = ERDDAP(server=ERDDAP_SERVER, protocol="tabledap")
    e.dataset_id = dataset_id
    e.variables = ERDDAP_VARIABLES
    return e.to_xarray()


def main() -> int:
    summary = []
    for buoy, dataset_id in DATASET_IDS.items():
        path = netcdf_path(buoy)
        if already_fetched(path):
            print(f"{buoy}: already fetched at {path}, skipping")
            ds = xr.open_dataset(path)
        else:
            print(f"{buoy}: fetching {dataset_id} from {ERDDAP_SERVER} ...")
            ds = fetch_buoy(buoy, dataset_id)
            ds.to_netcdf(path, format="NETCDF4")

            magic = path.read_bytes()[:4]
            if magic != HDF5_MAGIC:
                raise RuntimeError(
                    f"{buoy}: expected HDF5 magic bytes {HDF5_MAGIC!r} after "
                    f"to_netcdf(format='NETCDF4'), got {magic!r} instead — "
                    "virtualizarr's HDFParser will not be able to read this."
                )
            print(f"{buoy}: wrote {path} ({path.stat().st_size:,} bytes), confirmed HDF5")

        n = ds.sizes.get("row", ds.sizes.get("obs", next(iter(ds.sizes.values()), 0)))
        tmin = ds["time"].values.min() if "time" in ds.coords or "time" in ds.variables else None
        tmax = ds["time"].values.max() if "time" in ds.coords or "time" in ds.variables else None
        summary.append((buoy, n, tmin, tmax, path.stat().st_size))

    print("\nSummary:")
    for buoy, n, tmin, tmax, size in summary:
        print(f"  {buoy}: n={n} time={tmin} -> {tmax}  ({size:,} bytes)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
