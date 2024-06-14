import rioxarray  # noqa
import xarray as xr
from ndpyramid import pyramid_reproject, pyramid_resample


def make_pyramid_resample(levels: int):
    # ds = xr.tutorial.open_dataset("air_temperature")

    files = [
        "tests/data/3B-DAY.MS.MRG.3IMERG.20230729-S000000-E235959.V07B.nc4",
        "tests/data/3B-DAY.MS.MRG.3IMERG.20230730-S000000-E235959.V07B.nc4",
    ]
    ds = xr.open_mfdataset(
        files, engine="netcdf4", drop_variables=["time_bnds"], decode_coords="all"
    )

    ds = ds.chunk({"lat": 10, "lon": 10})
    ds = ds[["precipitation"]]

    ds = ds.rio.write_crs("EPSG:4326")
    # ds = ds.drop_vars('spatial_ref')

    ds = ds.transpose("time", "lat", "lon")

    # ds = ds.rename({"lon": "longitude", "lat": "latitude"})
    # ds = ds.rio.write_crs("EPSG:4326")
    ds = ds.isel(time=slice(0, 2))
    # import pdb; pdb.set_trace()
    return pyramid_resample(ds, levels=levels, x="lon", y="lat")


def make_pyramid_reproject(levels: int):
    ds = xr.tutorial.open_dataset("air_temperature")

    ds = ds.rename({"lon": "longitude", "lat": "latitude"})
    ds = ds.rio.write_crs("EPSG:4326")
    ds = ds.isel(time=slice(0, 2))

    return pyramid_reproject(ds, levels=levels)
