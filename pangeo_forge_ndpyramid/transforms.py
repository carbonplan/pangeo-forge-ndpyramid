from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Tuple, Union

import apache_beam as beam
import xarray as xr
import zarr # type: ignore
from pangeo_forge_recipes.patterns import Dimension, Index
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    RequiredAtRuntimeDefault,
    StoreToZarr,
)
from pangeo_forge_recipes.writers import ZarrWriterMixin

from .common import create_pyramid


@dataclass
class CreatePyramid(beam.PTransform):
    level: int
    epsg_code: Optional[str] = None
    rename_spatial_dims: Optional[dict] = None
    pixels_per_tile: Optional[int] = 128
    pyramid_method: Literal["coarsen", "regrid", "reproject", "resample"] = "reproject"
    pyramid_kwargs: Optional[dict] = field(default_factory=dict)

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.MapTuple(
            lambda k, v: (
                k,
                create_pyramid(
                    v,
                    level=self.level,
                    epsg_code=self.epsg_code,
                    rename_spatial_dims=self.rename_spatial_dims,
                    pyramid_method=self.pyramid_method,
                    pyramid_kwargs=self.pyramid_kwargs,
                ),
            )
        )


@dataclass
class StoreToPyramid(beam.PTransform, ZarrWriterMixin):
    """Store a PCollection of Xarray datasets as a Zarr Pyramid.
    :param levels: Number of pyramid levels to generate
    :param combine_dims: The dimensions to combine
    :param store_name: Zarr store will be created with this name under ``target_root``.
    :param other_chunks: Chunks for non-spatial dims.
    Spatial dims are controlled by pixels_per_tile. Default is None
      If a dimension is a not named, the chunks will be inferred from the data.
    :param target_root: Root path the Zarr store will be created inside;
        `store_name` will be appended to this prefix to create a full path.
    :param epsg_code: EPSG code to set dataset CRS if CRS missing. If provided will overwrite.
        Default is None.
    :param rename_spatial_dims: Dict containing the new spatial dim names for Rioxarray.
        Default is None.
    :param pyramid_method: type of pyramiding operation. ex: 'coarsen', 'resample', 'regrid', 'reproject'.
        Default is 'reproject'
    :param pyramid_kwargs: Dict containing any kwargs that should be passed to ndpyramid.
        Default is None.

    """

    levels: int
    combine_dims: List[Dimension]
    store_name: str
    pixels_per_tile: Optional[int] = 128
    other_chunks: Dict[str, int] = field(default_factory=dict)
    target_root: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )
    epsg_code: Optional[str] = None
    rename_spatial_dims: Optional[dict] = None
    pyramid_method: Literal["coarsen", "regrid", "reproject", "resample"] = "reproject"

    pyramid_kwargs: Optional[dict] = field(default_factory=dict)

    def expand(
        self,
        datasets: beam.PCollection[Tuple[Index, xr.Dataset]],
    ) -> beam.PCollection[zarr.storage.FSStore]:
        # Add multiscales metadata to the root of the target store
        from ndpyramid.utils import get_version, multiscales_template # type: ignore

        save_kwargs = {"levels": self.levels, "pixels_per_tile": self.pixels_per_tile}
        attrs = {
            "multiscales": multiscales_template(
                datasets=[
                    {"path": str(i), "pixels_per_tile": self.pixels_per_tile}
                    for i in range(self.levels)
                ],
                type="reduce",
                method=f"pyramid_{self.pyramid_method}",
                version=get_version(),
                kwargs=save_kwargs,
            )
        }
        # from StoreToZarr
        # target_chunks: 'Dict[str, int]' = <factory>,
        chunks = {"x": self.pixels_per_tile, "y": self.pixels_per_tile}
        if self.other_chunks is not None:
            chunks |= self.other_chunks

        ds = xr.Dataset(attrs=attrs)

        # Note: mypy typing in not happy here. 

        target_path = (self.target_root / self.store_name).get_mapper() # type: ignore 
        ds.to_zarr(store=target_path, compute=False)  # noqa

        # generate all pyramid levels
        lvl_list = list(range(0, self.levels))
        transform_pyr_lvls = []
        for lvl in lvl_list:
            pyr_ds = datasets | f"Create Pyr level: {str(lvl)}" >> CreatePyramid(
                level=lvl,
                epsg_code=self.epsg_code,
                rename_spatial_dims=self.rename_spatial_dims,
                pyramid_method=self.pyramid_method,
                pyramid_kwargs=self.pyramid_kwargs,
            )
            zarr_pyr_path = (
                pyr_ds
                | f"Store Pyr level: {lvl}"
                >> StoreToZarr(
                    target_root=self.target_root,
                    target_chunks=chunks,  # noqa
                    store_name=f"{self.store_name}/{str(lvl)}",
                    combine_dims=self.combine_dims,
                )
                | f"Consolidate Dimension Coordinates for: {lvl}"
                >> ConsolidateDimensionCoordinates()
            )

            transform_pyr_lvls.append(zarr_pyr_path)

        # To consolidate the top level metadata, we need all the pyramid groups to be written.
        # We are collecting all the pyramid level paths,
        # doing a global combine to fake an AWAIT call,
        # then consolidating the metadata

        consolidated_path = (
            transform_pyr_lvls
            | beam.Flatten()
            | beam.combiners.Sample.FixedSizeGlobally(1)
            | beam.Map(lambda x: target_path)
            | ConsolidateMetadata()
        )

        return consolidated_path
