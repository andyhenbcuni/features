#!/usr/bin/env bash
set -euo pipefail

## Gen dags
pip install --upgrade uv
uv sync --group pipelines --group pipelines-airflow --python 3.12
DAG_DIR="generated-dags/"
export PYTHONPATH=.
export DYNACONF_DAGS_DIR=${DAG_DIR}
mkdir -p ${DAG_DIR}

echo && echo "Generating DAG pipelines.. ⏳"
uv run python src/scripts/create_dag_files.py

echo && echo "Pushing DAG files to the bucket gs://${BUCKET_PATH}.. ⏳"
gsutil -m rsync -d -r -c ${DAG_DIR} gs://${BUCKET_PATH}
echo && echo "DAG files were successfully pushed to the bucket!  ✅"

echo && echo "Authenticating with Artifact Registry.. ⏳"
echo "${AR_SA_KEY}" > cred.json && gcloud auth activate-service-account --quiet --key-file=cred.json

readonly commit_hash=$(cat .git/ref | cut -c -7)
echo && echo "Tagging the image built from commit ${commit_hash} with value ${DEPLOY_ENV}.. ⏳"
gcloud container images add-tag --quiet "${IMAGE_PATH}:${commit_hash}" "${IMAGE_PATH}:${DEPLOY_ENV}"
echo && echo "The image was successfully tagged! ✅"
