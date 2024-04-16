FEEDSTOCK_NAME = "ww3_feedstock"  # Can we get this at runtime?
c.Bake.prune = 0
c.Bake.bakery_class = "pangeo_forge_runner.bakery.dataflow.DataflowBakery"
c.DataflowBakery.machine_type = "n2-highmem-32" # trying this now that all the files are downloaded
c.DataflowBakery.max_num_workers = 20
c.DataflowBakery.use_dataflow_prime = False
c.DataflowBakery.use_public_ips = True
c.DataflowBakery.service_account_email = (
    "julius-leap-dataflow@leap-pangeo.iam.gserviceaccount.com"
)
c.DataflowBakery.project_id = "leap-pangeo"
c.DataflowBakery.temp_gcs_location = f"gs://leap-scratch/data-library/feedstocks/temp/{FEEDSTOCK_NAME}"
c.TargetStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.InputCacheStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.TargetStorage.root_path = f"gs://leap-scratch/data-library/feedstocks/output/{FEEDSTOCK_NAME}/{{job_name}}"
c.InputCacheStorage.root_path = f"gs://leap-scratch/data-library/feedstocks/cache"
