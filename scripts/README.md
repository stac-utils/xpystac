# Test-fixture generation scripts

One-time scripts that generated the live data `tests/test_osn_live_custom_s3.py`
reads from `s3://esip/rsignell/icechunk_demo/` on OSN (`https://usgs.osn.mghpcc.org`,
a public, anonymously-readable bucket). Not part of the installed `xpystac` package
or the pytest-collected test suite — their own dependencies are listed here, not in
`pyproject.toml`.

Fetches the 7 "classic" NERACOOS/GoMOOS wave buoys (`A01`, `B01`, `E01`, `F01`,
`I01`, `M01`, `N01`) from NERACOOS's ERDDAP, converts each to real NetCDF4 (ERDDAP's
tabledap `.nc` is classic NetCDF-3, which virtualizarr's `HDFParser` can't read),
uploads them, builds one virtual Icechunk store per buoy with `virtualizarr`, and
writes/uploads a stac-geoparquet catalog describing all 7.

Requires AWS profile `osn-esip` in `~/.aws/credentials` with write access to the
`esip` bucket — only needed to *write* the fixture; the resulting test reads it
anonymously.

Each script is independently idempotent (safe to re-run):

```bash
uv run --with erddapy --with xarray --with h5netcdf --with h5py \
    python scripts/01_fetch_and_convert_buoys.py

uv run --with boto3 --with icechunk --with virtualizarr --with obstore \
    --with obspec_utils --with xarray --with h5netcdf --with h5py \
    python scripts/02_upload_and_virtualize.py

uv run --with boto3 --with rustac --with pystac --with xarray --with h5netcdf \
    --with h5py python scripts/03_build_stac_and_geoparquet.py
```
