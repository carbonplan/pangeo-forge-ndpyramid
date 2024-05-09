from typing import Literal, Optional, Tuple

import xarray as xr
import zarr
from pangeo_forge_recipes.patterns import Index


def create_pyramid(
    item: Tuple[Index, xr.Dataset],
    level: int,
    epsg_code: Optional[str] = None,
    rename_spatial_dims: Optional[dict] = None,
    projection: Literal["coarsen", "regrid", "reproject"] = "reproject",
    pyramid_kwargs: Optional[dict] = {},
) -> zarr.storage.FSStore:
    index, ds = item
    from ndpyramid.reproject import level_reproject
    from ndpyramid.utils import set_zarr_encoding

    if projection != "reproject":
        raise NotImplementedError("Only reproject is currently supported.")

    if epsg_code:
        import rioxarray  # noqa

        ds = ds.rio.write_crs(f"EPSG:{epsg_code}")

    # Ideally we can use ds = ds.anom.rio.set_spatial_dims(x_dim='lon',y_dim='lat')
    # But rioxarray.set_spatial_dims seems to only operate on the dataarray level
    # For now, we can use ds.rename
    if rename_spatial_dims:
        ds = ds.rename(rename_spatial_dims)

    level_ds = level_reproject(ds, level=level, **pyramid_kwargs)

    level_ds = set_zarr_encoding(
        level_ds,
        float_dtype="float32",
        int_dtype="int32",
        datetime_dtype="float32",
        object_dtype="str",
    )
    return index, level_ds
