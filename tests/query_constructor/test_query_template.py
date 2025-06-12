import pathlib
from collections.abc import Callable

import pytest

from src.actions import actions
from src.query_constructor import query_template, template_registry


class TestQueryTemplates:
    def test_renders_environment_template_fields(self) -> None:
        stub_query_template = '{{ stub_environment_variable }}'
        stub_environment_template_fields: dict[str, str] = {
            'stub_environment_variable': 'stub_value'
        }

        template = query_template.QueryTemplate(
            template=stub_query_template,
            environment_template_fields=stub_environment_template_fields,
        )

        actual_query: str = template.render(run_day='unused')

        assert actual_query == stub_environment_template_fields['stub_environment_variable']

    def test_renders_user_defined_template_fields(self) -> None:
        stub_query_template = '{{ stub_user_defined_variable }}'
        stub_user_defined_template_fields: dict[str, str] = {
            'stub_user_defined_variable': 'stub_value'
        }

        template = query_template.QueryTemplate(
            template=stub_query_template,
            user_defined_template_fields=stub_user_defined_template_fields,
        )

        actual_query: str = template.render(run_day='unused')

        assert actual_query == stub_user_defined_template_fields['stub_user_defined_variable']

    def test_environment_template_field_overrides_user_defined_template_field(
        self,
    ) -> None:
        stub_query_template = '{{ stub_variable }}'
        stub_user_defined_template_fields: dict[str, str] = {'stub_variable': 'wrong_value'}
        stub_environment_template_fields: dict[str, str] = {'stub_variable': 'stub_value'}

        template = query_template.QueryTemplate(
            template=stub_query_template,
            user_defined_template_fields=stub_user_defined_template_fields,
            environment_template_fields=stub_environment_template_fields,
        )

        actual_query: str = template.render(run_day='unused')

        assert actual_query == stub_environment_template_fields['stub_variable']

    def test_renders_run_day(self) -> None:
        stub_query_template = '{{ run_day }}'

        template = query_template.QueryTemplate(
            template=stub_query_template,
        )

        actual_query: str = template.render(run_day='stub_run_day')

        assert actual_query == 'stub_run_day'

    def test_raises_if_run_day_specified_via_user_or_environment_template_fields(self) -> None:
        stub_query_template = '{{ run_day }}'
        stub_user_defined_template_fields: dict[str, str] = {'run_day': 'wrong_value'}
        stub_environment_template_fields: dict[str, str] = {'run_day': 'another_wrong_value'}

        with pytest.raises(
            expected_exception=AttributeError, match='.*not allowed to contain a runtime.*'
        ):
            _ = query_template.QueryTemplate(
                template=stub_query_template,
                user_defined_template_fields=stub_user_defined_template_fields,
                environment_template_fields=stub_environment_template_fields,
            )

    def test_can_pull_template_from_registry(self) -> None:
        expected_query = 'stub_query'
        stub_templates: dict[str, Callable[..., str]] = {'stub_template': lambda: expected_query}
        stub_registry = template_registry.QueryTemplateRegistry(templates=stub_templates)

        template: query_template.QueryTemplate = query_template.QueryTemplate.from_registry(
            name='stub_template', query_template_registry=stub_registry
        )

        actual_query: str = template.render(run_day='unused')

        assert actual_query == expected_query

    def test_recursive_template_query_generation(self) -> None:
        stub_query_path: pathlib.Path = (
            pathlib.Path(__file__).parent / 'recursive_template_mocks' / 'mock_recursive_task.yaml'
        )
        expected_base_query_path: pathlib.Path = (
            pathlib.Path(__file__).parent
            / 'recursive_template_mocks'
            / 'mock_recursive_task_base_query.sql.jinja2'
        )
        environment_template_fields: dict[str, str] = {
            'start_date': '2023-01-01',
            'table_name': 'stub_table',
            'algo_project': 'algo_project',
            'dataset': 'dataset',
        }
        # placeholder user defined fields to pull the recursive template for test's assertion
        mock_user_defined_fields: dict[str, str] = {
            'accumulation_method': '',
            'aggregation_fields': '',
            'query': '',
            'partition_field': '',
            'join_key': '',
        }
        expected_query_template: query_template.QueryTemplate = (
            query_template.QueryTemplate.from_registry(
                name='recursive_template',
                environment_template_fields=environment_template_fields,
                user_defined_template_fields=mock_user_defined_fields,
            )
        )
        with expected_base_query_path.open() as f:
            expected_base_query: str = f.read()

        actual_query_template: query_template.QueryTemplate = actions.get_query_template(
            query_path=stub_query_path, environment_template_fields=environment_template_fields
        )
        assert actual_query_template.template == expected_query_template.template
        assert actual_query_template.user_defined_template_fields['query'] == expected_base_query

    @pytest.mark.regression
    def test_run_time_template_fields_override_environment_template_fields(self) -> None:
        stub_template = '{{ table_name }}'

        template = query_template.QueryTemplate(
            template=stub_template, environment_template_fields={'table_name': 'stub_table_name'}
        )

        actual_query: str = template.render(
            run_day='unused', run_time_template_fields={'table_name': 'stub_run_time_table_name'}
        )

        assert actual_query == 'stub_run_time_table_name'
