from importlib.metadata import version as _version

try:
    __version__ = _version("pangeo-forge-ndpyramid")
except Exception:
    __version__ = "9999"
