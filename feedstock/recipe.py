"""
Wave Watch 3
"""

import zarr
import os
from dataclasses import dataclass
import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
    T,
    Indexed,
)
from ruamel.yaml import YAML

yaml = YAML(typ="safe")


# copied from cmip feedstock (TODO: move to central repo?)
@dataclass
class Copy(beam.PTransform):
    target_prefix: str

    def _copy(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        import zarr
        import gcsfs

        # We do need the gs:// prefix?
        # TODO: Determine this dynamically from zarr.storage.FSStore
        source = f"gs://{os.path.normpath(store.path)}/"  # FIXME more elegant. `.copytree` needs trailing slash
        target = os.path.join(*[self.target_prefix] + source.split("/")[-2:])
        # gcs = gcsio.GcsIO()
        # gcs.copytree(source, target)
        print(f"HERE: Copying {source} to {target}")
        fs = gcsfs.GCSFileSystem()  # FIXME: How can we generalize this?
        fs.cp(source, target, recursive=True)
        # return a new store with the new path that behaves exactly like the input
        # to this stage (so we can slot this stage right before testing/logging stages)
        return zarr.storage.FSStore(target)

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | "Copying Store" >> beam.Map(self._copy)


# TODO: Both these stages are generally useful. They should at least be in the utils package, maybe in recipes?

# load the global config values (we will have to decide where these ultimately live)
catalog_meta = yaml.load(open("feedstock/catalog.yaml"))
copied_data_store_prefix = catalog_meta["data_store_prefix"]


# Set up injection attributes
# This is for demonstration purposes only and should be discussed with the broader LEAP/PGF community
# - Bake in information from the top level of the meta.yaml
# - Add a timestamp
# - Add the git hash
# - Add link to the meta.yaml on main
# - Add the recipe id


years = range(1993, 2023)
months = range(1, 13)
dates = []
for y in years:
    for m in months:
        dates.append((y, m))


def make_full_path(date: tuple[int, int]):
    year, month = date
    return f"https://data-dataref.ifremer.fr/ww3/GLOBMULTI_ERA5_GLOBCUR_01/GLOB-30M/{year}/FIELD_NC/LOPS_WW3-GLOB-30M_{year}{month:02d}.nc"


input_urls = [make_full_path(date) for date in dates]
pattern = pattern_from_file_sequence(input_urls, concat_dim="time")


# does this succeed with all coords stripped?
class StripCoords(beam.PTransform):
    @staticmethod
    def _strip_all_coords(item: Indexed[T]) -> Indexed[T]:
        """
        Many netcdfs contain variables other than the one specified in the `variable_id` facet.
        Set them all to coords
        """
        index, ds = item
        print(f"Preprocessing before {ds =}")
        ds = ds.reset_coords(drop=True)
        ds = ds.set_coords("MAPSTA")
        print("Testing: Drop variables")
        ds = ds.drop([v for v in ds.data_vars if v not in ['vcur']])
        print(f"Preprocessing after {ds =}")
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | "Debug: Remove coordinates" >> beam.Map(self._strip_all_coords)


WW3 = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(max_concurrency=5) #Might need to lower this even furtherto get this to not fail during caching.
    | OpenWithXarray()
    | StripCoords()
    | StoreToZarr(
        store_name="wavewatch3.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 100},
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target_prefix=copied_data_store_prefix)
)
