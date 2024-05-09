import fsspec
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.storage import FSSpecTarget

from .data_generation import make_pyramid


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
def pyramid_datatree(levels: int = 2):
    return make_pyramid(levels=levels)


@pytest.fixture(scope="session")
def create_file_pattern():
    return pattern_from_file_sequence(
        [str(path) for path in ["data/nc1.nc", "data/nc2.nc"]],
        concat_dim="time",
        nitems_per_file=1,
    )
