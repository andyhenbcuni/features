from contextlib import nullcontext as does_not_raise
from unittest import mock


def test_retrieves_templates_by_name():
    from src.query_constructor import template_registry

    registry = template_registry.QueryTemplateRegistry(
        templates={'stub_template': lambda: 'stub_value'}
    )

    assert registry.get_template('stub_template') == 'stub_value'


def test_templates_loaded_from_path_are_accessible_by_name(tmp_path):
    tmp_file = tmp_path / 'stub_template.jinja2'
    tmp_file.touch()

    with tmp_file.open('w') as f:
        f.write('stub_value')

    with mock.patch(
        'src.query_constructor.template_registry.DEFAULT_TEMPLATES_DIRECTORY', tmp_file.parent
    ):
        from src.query_constructor import template_registry

        registry = template_registry.QueryTemplateRegistry()

        with does_not_raise():
            assert registry.get_template('stub_template') == 'stub_value'
