from typing import Literal, Optional

import xarray as xr
import zarr  # type: ignore


def create_pyramid(
    ds: xr.Dataset,
    level: int,
    epsg_code: Optional[str] = None,
    rename_spatial_dims: Optional[dict] = None,
    pixels_per_tile: Optional[int] = 128,
    pyramid_method: Literal["coarsen", "regrid", "reproject", "resample"] = "reproject",
    pyramid_kwargs: Optional[dict] = {},
) -> zarr.storage.FSStore:
    from ndpyramid.utils import set_zarr_encoding  # type: ignore

    if pyramid_method not in ["reproject", "resample"]:
        raise NotImplementedError(
            "Only resample and reproject are currently supported."
        )

    if epsg_code:
        import rioxarray  # noqa

        ds = ds.rio.write_crs(f"EPSG:{epsg_code}")

    # Ideally we can use ds = ds.anom.rio.set_spatial_dims(x_dim='lon',y_dim='lat')
    # But rioxarray.set_spatial_dims seems to only operate on the dataarray level
    # For now, we can use ds.rename
    if rename_spatial_dims:
        ds = ds.rename(rename_spatial_dims)

    if pyramid_method == "reproject":
        from ndpyramid.reproject import level_reproject  # type: ignore

        level_ds = level_reproject(ds, level=level, **pyramid_kwargs)

    elif pyramid_method == "resample":
        from ndpyramid.resample import level_resample  # type: ignore

        # Should we have resample specific kwargs we pass here?
        level_ds = level_resample(
            ds, level=level, pixels_per_tile=pixels_per_tile, **pyramid_kwargs
        )

    level_ds = set_zarr_encoding(
        level_ds,
        float_dtype="float32",
        int_dtype="int32",
        datetime_dtype="float32",
        object_dtype="str",
    )
    return level_ds
