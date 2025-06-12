import logging
import re
from pathlib import Path


def get_yaml_config_paths(exclude_sql: bool = True) -> list[Path]:
    """
    Get all YAML config paths, optionally excluding those related to SQL.
    """
    paths = (Path(__file__).parent.parent / 'configs').rglob('*.yaml')
    if exclude_sql:
        return [path for path in paths if 'sql' not in path.parts]
    return list(paths)


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


logger: logging.Logger = get_logger('utils')


def update_markdown_section(
    path: Path,
    section_header: str,
    content: str | list[str],
    code_block_syntax: str | None = None,
) -> None:
    markdown: str = load_file(path=path)
    section_content: list[str] = _get_section_content(
        markdown=markdown,
        section_header=section_header,
    )
    processed_content: str = _preprocess_content(
        content=content, code_block_syntax=code_block_syntax
    )
    if not section_content:
        _create_markdown_section(
            path=path, section_header=section_header, content=processed_content
        )
    elif section_content != processed_content:
        _replace_markdown_section(
            path=path,
            section_header=section_header,
            markdown=markdown,
            replacement_content=processed_content,
        )
    else:
        logger.info(f'No updates required for {path.name}.')


def _preprocess_content(content: str | list[str], code_block_syntax: str | None) -> str:
    template: str = (
        f'```{code_block_syntax}\n{{content}}\n```' if code_block_syntax else f'{content}'
    )
    match content:
        case str():
            return template.format(content=content)
        case list():
            return '\n'.join([template.format(content=item) for item in content])
        case _:
            raise ValueError('Incorrect content specification. Must be of type: str or list[str].')


def _create_markdown_section(path: Path, section_header: str, content: str) -> None:
    section: str = f'{section_header}\n{content}\n'
    with path.open(mode='a') as file:
        file.write(section)


def _replace_markdown_section(
    path: Path, markdown: str, section_header: str, replacement_content: str
) -> None:
    pattern: str = _get_section_pattern(section_header=section_header)
    replacement_pattern: str = rf'{section_header}\n{replacement_content}\n'
    with path.open(mode='w') as file:
        file.write(re.sub(pattern=pattern, repl=replacement_pattern, string=markdown))


def load_file(path: Path) -> str:
    with path.open(mode='r') as file:
        return file.read()


def _get_section_content(markdown: str, section_header: str) -> list[str]:
    return re.findall(pattern=_get_section_pattern(section_header=section_header), string=markdown)


def _get_section_pattern(section_header: str) -> str:
    return f'^{section_header}\n(.*(?:\n(?!#).*)*)'
