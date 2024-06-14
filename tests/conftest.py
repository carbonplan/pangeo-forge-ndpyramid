import fsspec
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.storage import FSSpecTarget

from .data_generation import make_pyramid_reproject, make_pyramid_resample


@pytest.fixture
def pipeline():
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


@pytest.fixture()
def tmp_target(tmpdir_factory):
    fs = fsspec.get_filesystem_class("file")()
    path = str(tmpdir_factory.mktemp("target"))
    return FSSpecTarget(fs, path)


@pytest.fixture(scope="session")
def pyramid_datatree_reproject(levels: int = 2):
    return make_pyramid_reproject(levels=levels)


@pytest.fixture(scope="session")
def pyramid_datatree_resample(levels: int = 2):
    return make_pyramid_resample(levels=levels)


@pytest.fixture(scope="session")
def create_file_pattern():
    return pattern_from_file_sequence(
        [str(path) for path in ["tests/data/nc1.nc", "tests/data/nc2.nc"]],
        concat_dim="time",
        nitems_per_file=1,
    )


@pytest.fixture(scope="session")
def create_file_pattern_gpm_imerg():
    return pattern_from_file_sequence(
        [
            str(path)
            for path in [
                "tests/data/3B-DAY.MS.MRG.3IMERG.20230729-S000000-E235959.V07B.nc4",
                "tests/data/3B-DAY.MS.MRG.3IMERG.20230730-S000000-E235959.V07B.nc4",
            ]
        ],
        concat_dim="time",
        nitems_per_file=1,
    )
