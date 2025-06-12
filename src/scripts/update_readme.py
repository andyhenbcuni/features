from src.common import environment, paths
from src.pipelines import adapters, pipeline
from src.scripts import utils


def main() -> None:
    diagrams: list[str] = [
        f'```mermaid\n{pipeline.Pipeline.from_config(config_name).compile(adapter=adapters.Adapters.MERMAID)}\n```'
        for config_name in paths.get_config_names()
    ]

    utils.update_markdown_section(
        path=environment.CONFIG_DIR / 'README.md',
        section_header='## Pipelines Currently Managed by algo features',
        content='\n'.join(diagrams),
    )


if __name__ == '__main__':
    main()
