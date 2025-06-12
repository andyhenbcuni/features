# Algo Features

Algo Features is a modular monolith consisting of 6 different components:

>__1. Pipelines Library:__ Platform-agnostic interface for defining and deploying >batch pipelines
>
>__2. Actions SDK:__ Library of commonly used actions, interface to internal services
>
>__3. Managed Table Service:__ Platform-agnostic state machine for autonomously updating and backfilling tables
>
>__4. Query Constructor:__ optimized SQL generation for common etl tasks, also manages user templated sql
>
>__5. Configs:__ declarative pipeline definitions that are deployed via CICD
>
>__6. Scripts:__ utility scripts for deployment etc

## Documentation

See `doc/pipeline_from_scratch.ipynb` for a tutorial on how to build pipelines in algo features.
See `doc/updating_existing_pipeline.ipynb` for a tutorial on how to update pipelines in algo features.

See `doc/deploy_to_stage.md` for a tutorial on how to deploy to stage .


## Setup

1. Clone repository

 ```shell
git clone git@github.com:NBCUDTC/src-algo-features.git
```

2. Create virtual env and install dependencies, tools, and pre-commit hooks

```shell
make setup
```


## Important Links


### Concourse
[Concourse Pipeline](https://concourse-mgmt.nbcupea.mgmt.nbcuott.com/teams/ds-algo/pipelines/algo_features)

### BigQuery
[Dev BigQuery](https://console.cloud.google.com/bigquery?project=nbcu-ds-algo-int-001&ws=!1m4!1m3!3m2!1snbcu-ds-algo-int-001!2salgo_features)

[Stage BigQuery](https://console.cloud.google.com/bigquery?project=nbcu-ds-algo-int-nft-001&ws=!1m4!1m3!3m2!1snbcu-ds-algo-int-nft-001!2salgo_features)

[Prod BigQuery](https://console.cloud.google.com/bigquery?project=nbcu-ds-algo-prod-001&ws=!1m4!1m3!3m2!1snbcu-ds-algo-prod-001!2salgo_features)


### Airflow
[Dev Airflow](https://airflow-algo-int-001-nbcu-ds-stable-int.nbcupea.dev.nbcuott.com/home)

[Stage Airflow](https://airflow-algo-int-nft-nbcu-ds-int-nft.nbcupea.stage.nbcuott.com/home)

[Prod Airflow](https://airflow-algo-prod-nbcu-ds-prod.nbcupea.prod.nbcuott.com/home)


### Airflow Buckets
[Dev Airflow Bucket](https://console.cloud.google.com/storage/browser/airflow-algo-int-001-nbcu-ds-stable-int-001;tab=objects?forceOnBucketsSortingFiltering=true&project=nbcu-ds-stable-int-001&prefix=&forceOnObjectsSortingFiltering=false)

[Stage Airflow Bucket](https://console.cloud.google.com/storage/browser/airflow-algo-int-nft-nbcu-ds-int-nft-001;tab=objects?prefix=&forceOnObjectsSortingFiltering=false)

[Prod Airflow Bucket](https://console.cloud.google.com/storage/browser/airflow-algo-prod-nbcu-ds-prod-001?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&prefix=&forceOnObjectsSortingFiltering=false)


### GKE
[Dev K8 Workloads](https://console.cloud.google.com/kubernetes/workload/overview?project=nbcu-ds-stable-int-001&pageState=(%22savedViews%22:(%22i%22:%22116c5cfe5b494117a3e9785bee1a9e1c%22,%22c%22:[],%22n%22:[%22airflow-algo-int-001%22])))

[Stage K8 Workloads](https://console.cloud.google.com/kubernetes/workload/overview?project=nbcu-ds-int-nft-001&pageState=(%22savedViews%22:(%22i%22:%222d0d46a98a3f481d82d54b65db8027da%22,%22c%22:[],%22n%22:[%22airflow-algo-int-nft%22])))

[Prod K8 Workloads](https://console.cloud.google.com/kubernetes/workload/overview?project=nbcu-ds-prod-001&pageState=(%22savedViews%22:(%22i%22:%2221ba8f4f941044bfa2c6d12dc1f8b5c3%22,%22c%22:[],%22n%22:[%22airflow-algo-prod%22]),%22workload_list_table%22:(%22s%22:[(%22i%22:%22pods_sort_key%22,%22s%22:%221%22),(%22i%22:%22metadata%2Fname%22,%22s%22:%220%22)])))
