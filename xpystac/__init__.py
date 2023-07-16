from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("xpystac")
except PackageNotFoundError:  # noqa
    # package is not installed
    pass
