from dataclasses import dataclass, field
from typing import Any, NoReturn

from jinja2 import Template, environment, meta, nodes

from src.query_constructor import template_registry

RUNTIME_TEMPLATE_FIELDS: set[str] = {'run_day'}


class TemplateException(Exception):
    pass


def raise_template_exception(message: str) -> NoReturn:
    raise TemplateException(message)


jinja2_environment = environment.Environment()
jinja2_environment.globals['raise_template_exception'] = raise_template_exception


@dataclass
class QueryTemplate:
    template: str
    environment_template_fields: dict[str, Any] = field(default_factory=dict)
    user_defined_template_fields: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not RUNTIME_TEMPLATE_FIELDS.isdisjoint(self.provided_template_fields.keys()):
            raise AttributeError(
                f'Provided template are not allowed to contain a runtime template field. Found field: {RUNTIME_TEMPLATE_FIELDS.intersection(self.provided_template_fields.keys())}'
            )
        if not self.required_template_fields.issubset(self.provided_template_fields.keys()):
            raise AttributeError(
                f'Template is missing the following required fields: {self.required_template_fields - set(self.provided_template_fields)}'
            )

    def render(self, run_day: str, run_time_template_fields: dict[str, Any] | None = None) -> str:
        template_fields: dict[str, Any] = (
            self.provided_template_fields | {'run_day': run_day} | (run_time_template_fields or {})
        )
        template: Template = jinja2_environment.from_string(source=self.template)
        return template.render(**template_fields)

    @property
    def provided_template_fields(self) -> dict[str, Any]:
        return self.user_defined_template_fields | self.environment_template_fields

    @property
    def required_template_fields(self) -> set[str]:
        ast: nodes.Template = jinja2_environment.parse(source=self.template)
        return meta.find_undeclared_variables(ast=ast) - RUNTIME_TEMPLATE_FIELDS

    @classmethod
    def from_registry(
        cls,
        name: str,
        environment_template_fields: dict[str, Any] | None = None,
        user_defined_template_fields: dict[str, Any] | None = None,
        query_template_registry: template_registry.QueryTemplateRegistry | None = None,
    ):
        registry = query_template_registry or template_registry.QueryTemplateRegistry()
        template: str = registry.get_template(name=name)
        return cls(
            template=template,
            environment_template_fields=environment_template_fields or {},
            user_defined_template_fields=user_defined_template_fields or {},
        )
