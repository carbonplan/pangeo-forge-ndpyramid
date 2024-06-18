import os
from dataclasses import dataclass

import apache_beam as beam
import datatree as dt
import pytest
import xarray as xr
from datatree.testing import assert_isomorphic
from pangeo_forge_recipes.transforms import OpenWithXarray, StoreToZarr

from pangeo_forge_ndpyramid.transforms import StoreToPyramid


@pytest.mark.xfail(reason="local files not commited. ToDo")
def test_pyramid_resample(
    pyramid_datatree_resample, create_file_pattern_gpm_imerg, pipeline, tmp_target
):
    pattern = create_file_pattern_gpm_imerg

    @dataclass
    class Transpose(beam.PTransform):
        """Transpose dim order for pyresample"""

        def _transpose(self, ds: xr.Dataset) -> xr.Dataset:
            ds = ds[["precipitation"]]
            ds = ds.chunk({"time": 1, "lat": 100, "lon": 100})
            ds = ds.transpose("time", "lat", "lon")

            return ds

        def expand(self, pcoll):
            return pcoll | "Transpose" >> beam.MapTuple(
                lambda k, v: (k, self._transpose(v))
            )

    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | Transpose()
            | "Write Pyramid Levels"
            >> StoreToPyramid(
                target_root=tmp_target,
                store_name="pyramid",
                levels=2,
                epsg_code="4326",
                pyramid_method="resample",
                pyramid_kwargs={"x": "lon", "y": "lat"},
                combine_dims=pattern.combine_dim_keys,
            )
        )
    # import pdb; pdb.set_trace()

    pgf_dt = dt.open_datatree(
        os.path.join(tmp_target.root_path, "pyramid"),
        engine="zarr",
        consolidated=False,
        chunks={},
    )
    assert_isomorphic(
        pgf_dt, pyramid_datatree_resample
    )  # every node has same # of children
    xr.testing.assert_allclose(
        pgf_dt["0"].to_dataset(), pyramid_datatree_resample["0"].to_dataset()
    )
    xr.testing.assert_allclose(
        pgf_dt["1"].to_dataset(), pyramid_datatree_resample["1"].to_dataset()
    )


# TODO: Test names and attrs
def test_pyramid_reproject(
    pyramid_datatree_reproject,
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

        (
            process
            | "Write Pyramid Levels"
            >> StoreToPyramid(
                target_root=tmp_target,
                store_name="pyramid",
                levels=2,
                epsg_code="4326",
                pyramid_method="reproject",
                rename_spatial_dims={"lon": "x", "lat": "y"},
                combine_dims=pattern.combine_dim_keys,
            )
        )

    assert xr.open_dataset(
        os.path.join(tmp_target.root_path, "store"), engine="zarr", chunks={}
    )

    pgf_dt = dt.open_datatree(
        os.path.join(tmp_target.root_path, "pyramid"),
        engine="zarr",
        consolidated=False,
        chunks={},
    )
    assert_isomorphic(
        pgf_dt, pyramid_datatree_reproject
    )  # every node has same # of children
    xr.testing.assert_allclose(
        pgf_dt["0"].to_dataset(), pyramid_datatree_reproject["0"].to_dataset()
    )
    xr.testing.assert_allclose(
        pgf_dt["1"].to_dataset(), pyramid_datatree_reproject["1"].to_dataset()
    )
