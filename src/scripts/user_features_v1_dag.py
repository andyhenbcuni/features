import dataclasses
import functools
import json
from datetime import timedelta
from typing import Any

from airflow import models
from airflow.models import variable
from airflow.providers.google.cloud.operators import kubernetes_engine
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils import task_group
from dateutil import parser


# #### Slack Alerts ####
def task_failure_alert(context):
    SLACK_CONN_ID = 'slack_connection_id'

    slack_msg = f"""
    *red_circle*: Failed Task
    *Dag ID*: {context.get('task_instance').dag_id}
    *Run ID*: {context.get('task_instance').run_id}
    *Task Key*: {context.get('task_instance_key_str')}
    *Url*: {context.get('task_instance').log_url}
    """

    slack_hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    slack_hook.send(text=slack_msg)


@dataclasses.dataclass
class AirflowEnvironmentVariables:
    cluster_project: str = variable.Variable.get(key='CLUSTER_PROJECT')
    tag: str = variable.Variable.get(key='TAG')
    cluster_name: str = variable.Variable.get(key='CLUSTER_NAME')
    cluster_location: str = variable.Variable.get(key='CLUSTER_LOCATION')
    namespace: str = variable.Variable.get(key='NAMESPACE')
    algo_project: str = variable.Variable.get(key='ALGO_PROJECT')


environment_variables = AirflowEnvironmentVariables()


def get_operator(  # noqa: PLR0913
    task_id: str,
    config_path: str,
    pipe: str,
    action: str,
    parameters: dict[str, Any],
    dag: models.DAG,
    environment_variables: AirflowEnvironmentVariables,
) -> kubernetes_engine.GKEStartPodOperator:
    return kubernetes_engine.GKEStartPodOperator(
        task_id=task_id,
        cmds=[
            'python',
            'src/pipelines/entrypoint.py',
            '--config_path',
            config_path,
            '--pipe',
            pipe,
            '--action',
            action,
            '--parameters',
            json.dumps(obj=parameters | {'run_day': '{{ ds }}'}),
        ],
        env_vars={
            'AIRFLOW_VAR_ALGO_PROJECT': environment_variables.algo_project,
        },
        dag=dag,
        project_id=environment_variables.cluster_project,
        cluster_name=environment_variables.cluster_name,
        location=environment_variables.cluster_location,
        is_delete_operator_pod=True,
        namespace=environment_variables.namespace,
        gcp_conn_id='gcp_conn',
        service_account_name='algo-features-sa',
        use_internal_ip=True,
        image_pull_policy='Always',
        startup_timeout_seconds=1000,
        get_logs=True,
        log_events_on_failure=True,
        name='algo-features',
        image='us-west2-docker.pkg.dev/res-nbcupea-mgmt-003/algo-docker/offline:user_features_v1',
        retries=3,
        retry_delay=timedelta(hours=1),
        on_failure_callback=task_failure_alert,
    )


with models.DAG(
    dag_id='user_features',
    schedule_interval='0 12 * * *',
    start_date=parser.parse('2024-05-04'),
    max_active_runs=1,
    catchup=False,
    access_control={
        'Algo_Engineer': {'can_edit', 'can_read'},
        'Consci_Engineer': {'can_edit', 'can_read'},
    },
) as dag:
    operator = functools.partial(
        get_operator,
        dag=dag,
        environment_variables=environment_variables,
    )
    with task_group.TaskGroup(group_id='silver_video') as silver_video:
        silver_video_check_partition = operator(
            task_id='silver_video_check_partition',
            config_path='src/algo_features/configs/user_features.yaml',
            pipe='silver_video',
            action='check_partition',
            parameters={},
        )

    with task_group.TaskGroup(group_id='silver_user') as silver_user:
        silver_user_check_partition = operator(
            task_id='silver_user_check_partition',
            config_path='src/algo_features/configs/user_features.yaml',
            pipe='silver_user',
            action='check_partition',
            parameters={},
        )

    with task_group.TaskGroup(group_id='silver_video_daily_rollup') as silver_video_daily_rollup:
        silver_video_daily_rollup_default_action = operator(
            task_id='silver_video_daily_rollup_default_action',
            config_path='src/algo_features/configs/user_features.yaml',
            pipe='silver_video_daily_rollup',
            action='default_action',
            parameters={},
        )

    with task_group.TaskGroup(group_id='silver_user_daily_rollup') as silver_user_daily_rollup:
        silver_user_daily_rollup_default_action = operator(
            task_id='silver_user_daily_rollup_default_action',
            config_path='src/algo_features/configs/user_features.yaml',
            pipe='silver_user_daily_rollup',
            action='default_action',
            parameters={},
        )

    with task_group.TaskGroup(group_id='trial_features') as trial_features:
        trial_features_default_action = operator(
            task_id='trial_features_default_action',
            config_path='src/algo_features/configs/user_features.yaml',
            pipe='trial_features',
            action='default_action',
            parameters={},
        )

    with task_group.TaskGroup(
        group_id='silver_video_all_time_rollup'
    ) as silver_video_all_time_rollup:
        silver_video_all_time_rollup_default_action = operator(
            task_id='silver_video_all_time_rollup_default_action',
            config_path='src/algo_features/configs/user_features.yaml',
            pipe='silver_video_all_time_rollup',
            action='default_action',
            parameters={},
        )

    with task_group.TaskGroup(
        group_id='silver_video_daily_rollup_agg'
    ) as silver_video_daily_rollup_agg:
        silver_video_daily_rollup_agg_default_action = operator(
            task_id='silver_video_daily_rollup_agg_default_action',
            config_path='src/algo_features/configs/user_features.yaml',
            pipe='silver_video_daily_rollup_agg',
            action='default_action',
            parameters={},
        )

    with task_group.TaskGroup(group_id='user_features') as user_features:
        user_features_default_action = operator(
            task_id='user_features_default_action',
            config_path='src/algo_features/configs/user_features.yaml',
            pipe='user_features',
            action='default_action',
            parameters={},
        )

    silver_video >> silver_video_daily_rollup

    silver_user >> silver_user_daily_rollup

    silver_user >> trial_features

    silver_video_daily_rollup >> silver_video_all_time_rollup

    silver_video_daily_rollup >> silver_video_daily_rollup_agg

    silver_user_daily_rollup >> user_features

    silver_video_daily_rollup >> user_features

    silver_video_daily_rollup_agg >> user_features

    trial_features >> user_features

    silver_video_all_time_rollup >> user_features
