import rioxarray  # noqa
import xarray as xr
from ndpyramid import pyramid_reproject


def make_pyramid(levels: int):
    ds = xr.tutorial.open_dataset("air_temperature")
    ds = ds.rename({"lon": "longitude", "lat": "latitude"})
    ds = ds.rio.write_crs("EPSG:4326")
    ds = ds.isel(time=slice(0, 2))

    # other_chunks added to e2e pass of pyramid b/c target_chunks invert_meshgrid error
    return pyramid_reproject(ds, levels=levels, other_chunks={"time": 1})
