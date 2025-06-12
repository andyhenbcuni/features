import logging
import uuid
from pathlib import Path
from typing import Any, NoReturn

import jinja2


class TemplateException(Exception):
    pass


def raise_template_exception(message: str) -> NoReturn:
    raise TemplateException(message)


jinja2_environment = jinja2.environment.Environment()
jinja2_environment.globals['raise_template_exception'] = raise_template_exception


def read_template(template_path: Path, template_fields: dict[str, Any] | None = None) -> str:
    """Function for reading and formatting a jinja template.

    Args:
                    template_path (list[str]): path to the template
                    template_fields (Optional[dict], optional): fields to template. Defaults to None.

    Returns:
                    (str): formatted template
    """

    with template_path.open() as file:
        template: jinja2.Template = jinja2_environment.from_string(source=file.read())
        rendered: str = template.render(**(template_fields or {}))
    return rendered


class RenderMermaid:
    def __init__(self, diagram: str):
        self._diagram = self._process_diagram(diagram)
        self._uid = uuid.uuid4()

    @staticmethod
    def _process_diagram(diagram: str) -> str:
        _diagram = diagram.replace('\n', '\\n')
        _diagram = _diagram.lstrip('\\n')
        _diagram = _diagram.replace("'", '"')
        return _diagram

    def _repr_html_(self) -> str:
        ret = f"""
        <div class="mermaid-{self._uid}"></div>
        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10.1.0/+esm'
            const graphDefinition = \'_diagram\';
            const element = document.querySelector('.mermaid-{self._uid}');
            const {{ svg }} = await mermaid.render('graphDiv-{self._uid}', graphDefinition);
            element.innerHTML = svg;
        </script>
        """
        ret = ret.replace('_diagram', self._diagram)
        return ret


# TODO: mv to common
def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if not logger.hasHandlers():
        # define formatter
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

        # define file handler
        file_handler = logging.FileHandler(
            f'{name}.log',
            encoding='utf-8',
            mode='w',
        )
        log_handler(file_handler, formatter, logger)
        # define stream handler
        stream_handler = logging.StreamHandler()
        log_handler(stream_handler, formatter, logger)
    return logger


def log_handler(handler, formatter, logger):
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
