<p align="left" >
<a href='https://carbonplan.org'>
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://carbonplan-assets.s3.amazonaws.com/monogram/light-small.png">
  <img alt="CarbonPlan monogram." height="48" src="https://carbonplan-assets.s3.amazonaws.com/monogram/dark-small.png">
</picture>
</a>
</p>

# Pangeo-Forge-Ndpyramid

[Pangeo-Forge](https://pangeo-forge.readthedocs.io/en/latest/) extension library used to generate pyramids via [ndpyramid](https://github.com/carbonplan/ndpyramid)

[![CI](https://github.com/carbonplan/python-project-template/actions/workflows/main.yaml/badge.svg)](https://github.com/carbonplan/python-project-template/actions/workflows/main.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Usage

`pangeo-forge-ndpyramid` contains a apache-beam transform named `StoreToPyramid` which can be used with `pangeo-forge-recipes` to generate multiscale Zarr stores.
```python

from pangeo_forge_ndpyramid import StoreToPyramid

...

with beam.Pipeline() as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenURLWithFSSpec()
        | OpenWithXarray()
        | 'Write Pyramid Levels' >> StoreToPyramid(
            target_root=target_root,
            store_name='pyramid_store.zarr',
            epsg_code='4326',
            levels=3,
            combine_dims=pattern.combine_dim_keys,
        )
    )



```

## license

All the code in this repository is [MIT](https://choosealicense.com/licenses/mit/)-licensed, but we request that you please provide attribution if reusing any of our digital content (graphics, logo, articles, etc.).

## about us

CarbonPlan is a nonprofit organization that uses data and science for climate action. We aim to improve the transparency and scientific integrity of climate solutions with open data and tools. Find out more at [carbonplan.org](https://carbonplan.org/) or get in touch by [opening an issue](https://github.com/carbonplan/python-project-template/issues/new) or [sending us an email](mailto:hello@carbonplan.org).
