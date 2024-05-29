import os

import apache_beam as beam
import xarray as xr
from pangeo_forge_recipes.transforms import OpenWithXarray, StoreToZarr

from pangeo_forge_ndpyramid.transforms import StoreToPyramid


# TODO: We should parameterize the reprojection methods available in ndpyramid
# TODO: Test names and attrs 
def test_pyramid(
    pyramid_datatree,
    create_file_pattern,
    pipeline,
    tmp_target,
):
    pattern = create_file_pattern

    with pipeline as p:
        process = (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
        )

        process | "Write Base Level" >> StoreToZarr(
            target_root=tmp_target,
            store_name="store",
            combine_dims=pattern.combine_dim_keys,
        )
        process | "Write Pyramid Levels" >> StoreToPyramid(
            target_root=tmp_target,
            store_name="pyramid",
            levels=2,
            epsg_code="4326",
            rename_spatial_dims={"lon": "longitude", "lat": "latitude"},
            combine_dims=pattern.combine_dim_keys,
        )

    import datatree as dt
    from datatree.testing import assert_isomorphic

    assert xr.open_dataset(
        os.path.join(tmp_target.root_path, "store"), engine="zarr", chunks={}
    )

    pgf_dt = dt.open_datatree(
        os.path.join(tmp_target.root_path, "pyramid"),
        engine="zarr",
        consolidated=False,
        chunks={},
    )

    assert_isomorphic(pgf_dt, pyramid_datatree)  # every node has same # of children
    xr.testing.assert_allclose(
        pgf_dt["0"].to_dataset(), pyramid_datatree["0"].to_dataset()
    )
    xr.testing.assert_allclose(
        pgf_dt["1"].to_dataset(), pyramid_datatree["1"].to_dataset()
    )
