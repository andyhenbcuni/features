import dataclasses
import inspect
import json
import pathlib
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from airflow import models
from airflow.models import variable
from airflow.providers.google.cloud.operators import kubernetes_engine
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

from src.pipelines import pipeline, port, utils

TEMPLATE_PATH: pathlib.Path = pathlib.Path(__file__).parent / 'templates'
ACCESS_CONTROL: dict[str, set[str]] = {
    'Algo_Engineer': {'can_edit', 'can_read'},
    'Consci_Engineer': {'can_edit', 'can_read'},
}
SLACK_WEBHOOK = SlackWebhookHook(slack_webhook_conn_id='slack_connection_id')
EMOJI_MAP: dict[str, str] = {
    'failed': 'red-siren',
    'up_for_retry': 'warning',
    'success': 'check-green',
}


def get_slack_alert_from_context(context: dict[str, models.TaskInstance]):
    task_instance: models.TaskInstance = context['task_instance']
    state: str = task_instance.state or ''
    dag_id: str = task_instance.dag_id
    log_url: str = task_instance.log_url
    emoji: str = EMOJI_MAP[state]
    SLACK_WEBHOOK.send(
        text=f':{emoji}: {dag_id} *{state.replace("_", " ")}!*\nLogs: {log_url}',
        blocks=[
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': f':{emoji}: <{log_url}|{dag_id}> *{state.replace("_", " ")}!*',
                },
            }
        ],
    )


def sla_miss_callback(dag: models.DAG, *args):
    SLACK_WEBHOOK.send(
        text=f':warning: {dag.dag_id} *Exceeded an SLA!*',
        blocks=[
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': f':warning: {dag.dag_id} *Exceeded an SLA!*',
                },
            }
        ],
    )


def _default_args() -> dict[str, int | bool]:
    # Setting retries to a high number due to timeouts in kubernetes pods creating false negative failures.
    # On retry the pod is picked up again, for long running jobs, this means we need many retries.
    return {'retries': 15, 'depends_on_past': False, 'max_active_runs': 1}


@dataclasses.dataclass
class AirflowDAGConfig:
    default_args: dict[str, Any] = dataclasses.field(default_factory=_default_args)
    description: str | None = None
    max_active_runs: int = 1
    catchup: bool = False


# TODO: cmds deserve a test
def get_operator(
    task_id: str,
    action: str,
    parameters: dict[str, Any],
    retries: int = 3,
    retry_delay: timedelta = timedelta(minutes=1),
) -> kubernetes_engine.GKEStartPodOperator:
    return kubernetes_engine.GKEStartPodOperator(
        task_id=task_id,
        cmds=[
            'uv',
            'run',
            'python',
            'src/actions/actions.py',
            f'{action}',
            '--parameters',
            f'{json.dumps(obj=parameters)}',
        ],
        is_delete_operator_pod=True,
        gcp_conn_id='gcp_conn',
        use_internal_ip=True,
        image_pull_policy='Always',
        startup_timeout_seconds=1000,
        get_logs=True,
        log_events_on_failure=True,
        retries=retries,
        retry_delay=retry_delay,
        sla=timedelta(hours=24),
        on_failure_callback=get_slack_alert_from_context,
        on_retry_callback=get_slack_alert_from_context,
        env_vars={
            'AIRFLOW_VAR_ALGO_PROJECT': variable.Variable.get(key='ALGO_PROJECT'),
        },
        project_id=variable.Variable.get(key='CLUSTER_PROJECT'),
        cluster_name=variable.Variable.get(key='CLUSTER_NAME'),
        location=variable.Variable.get(key='CLUSTER_LOCATION'),
        namespace=variable.Variable.get(key='NAMESPACE'),
        service_account_name='algo-features-sa',
        name='algo-features',
        image=f'us-west2-docker.pkg.dev/res-nbcupea-mgmt-003/algo-docker/offline:{variable.Variable.get(key="TAG")}',
    )


# TODO: need abstraction for run time parameters that gets compiled
@dataclass
class Airflow:
    def compile(self, object: object) -> str:
        if isinstance(object, pipeline.Pipeline):
            return utils.read_template(
                template_path=TEMPLATE_PATH / 'dag.py.jinja2',  # TODO: should be a function
                template_fields={
                    'alerts': [
                        inspect.getsource(get_slack_alert_from_context),
                        inspect.getsource(sla_miss_callback),
                    ],
                    'access_control': ACCESS_CONTROL,
                    'operator': inspect.getsource(get_operator),
                    'pipeline': object,
                    'run_time_parameters': {'run_day': '{{ ds }}'},
                },
            )
        else:
            raise NotImplementedError

    def decompile(self, artifact: str, object: type) -> port.Port:
        raise NotImplementedError
