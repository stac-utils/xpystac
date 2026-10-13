"""Shared constants for the OSN/NERACOOS live test-fixture generation scripts.

Not part of the installed xpystac package — see scripts/README.md for how to
run these. Produces the fixture data tests/test_osn_live_custom_s3.py reads.
"""

from pathlib import Path

# --- OSN / esip bucket -------------------------------------------------
ENDPOINT = "https://usgs.osn.mghpcc.org"
BUCKET = "esip"
AWS_PROFILE = "osn-esip"
REGION = "us-east-1"  # placeholder; this profile has no region configured

DEMO_PREFIX = "rsignell/icechunk_demo"
NETCDF_PREFIX = f"{DEMO_PREFIX}/netcdf"
ICECHUNK_PREFIX = f"{DEMO_PREFIX}/icechunk"
CATALOG_KEY = f"{DEMO_PREFIX}/catalog.parquet"
SCHEME_KEY = "osn-esip"

# --- NERACOOS ERDDAP -----------------------------------------------------
ERDDAP_SERVER = "https://data.neracoos.org/erddap"
BUOY_IDS = ["A01", "B01", "E01", "F01", "I01", "M01", "N01"]
DATASET_IDS = {buoy: f"{buoy}_waves" for buoy in BUOY_IDS}
ERDDAP_VARIABLES = ["time", "latitude", "longitude", "significant_wave_height"]

# --- Local scratch (gitignored, not committed) ---------------------------
DATA_DIR = Path(__file__).parent / "fixtures_data"
DATA_DIR.mkdir(exist_ok=True)


def netcdf_path(buoy: str) -> Path:
    return DATA_DIR / f"{buoy}.nc"


def netcdf_href(buoy: str) -> str:
    return f"s3://{BUCKET}/{NETCDF_PREFIX}/{buoy}.nc"


# icechunk requires a VirtualChunkContainer's url_prefix to end in "/" (it's a
# directory-style prefix, not a single-file href) — all 7 buoys' NetCDF files
# share this one directory, so this one container/credentials-dict entry
# covers all of them. This must be the "netcdf" data asset's href in the STAC
# items too (not a per-buoy file path), since xpystac's
# construct_virtual_containers_config() builds its read-time container from
# the data asset's literal href.
NETCDF_DIR_HREF = f"s3://{BUCKET}/{NETCDF_PREFIX}/"


def icechunk_prefix(buoy: str) -> str:
    return f"{ICECHUNK_PREFIX}/{buoy}"


def icechunk_href(buoy: str) -> str:
    return f"s3://{BUCKET}/{icechunk_prefix(buoy)}"
