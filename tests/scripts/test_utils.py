import functools
from pathlib import Path
from typing import Final

import pytest

from src.scripts import utils

SECTION_HEADER: Final[str] = '# Test Section'
CONTENT: Final[str] = 'content'
CODE_BLOCK_SYNTAX: Final[str] = 'unused'
EXPECTED_SINGLE_CONTENT: Final[str] = f'{SECTION_HEADER}\n```{CODE_BLOCK_SYNTAX}\n{CONTENT}\n```\n'
EXPECTED_MULTIPLE_CONTENT: Final[str] = (
    f'{EXPECTED_SINGLE_CONTENT}```{CODE_BLOCK_SYNTAX}\n{CONTENT}\n```\n'
)


def write_file(path: Path, content: str) -> None:
    with path.open(mode='w+') as temp_file:
        temp_file.write(content)


update_markdown_section = functools.partial(
    utils.update_markdown_section,
    section_header=SECTION_HEADER,
    code_block_syntax=CODE_BLOCK_SYNTAX,
)


def update_markdown_section_single_content(path: Path) -> None:
    update_markdown_section(
        path=path.absolute(),
        content=CONTENT,
    )


def update_markdown_content_list_of_content(path: Path) -> None:
    update_markdown_section(
        path=path.absolute(),
        content=[CONTENT, CONTENT],
    )


def assert_path_content_matches_expected_content(path: Path, expected_content: str) -> None:
    with path.open(mode='r') as temp_file:
        assert temp_file.read() == expected_content


@pytest.fixture(scope='function')
def tmp_markdown(tmp_path: Path) -> Path:
    path: Path = tmp_path / 'test_markdown.md'
    path.touch()
    return path


class TestUpdateMarkdownFile:
    class TestNoExistingSections:
        def test_single_content(self, tmp_markdown: Path) -> None:
            update_markdown_section_single_content(path=tmp_markdown)

            assert_path_content_matches_expected_content(
                path=tmp_markdown,
                expected_content=EXPECTED_SINGLE_CONTENT,
            )

        def test_list_of_content(self, tmp_markdown: Path) -> None:
            update_markdown_content_list_of_content(path=tmp_markdown)

            assert_path_content_matches_expected_content(
                path=tmp_markdown,
                expected_content=EXPECTED_MULTIPLE_CONTENT,
            )

    class TestSectionForUpdateAlreadyExists:
        existing_section: str = f'{SECTION_HEADER}\n existing content'

        def test_single_content(self, tmp_markdown: Path) -> None:
            write_file(path=tmp_markdown, content=self.existing_section)

            update_markdown_section_single_content(path=tmp_markdown)

            assert_path_content_matches_expected_content(
                path=tmp_markdown,
                expected_content=EXPECTED_SINGLE_CONTENT,
            )

        def test_list_of_content(self, tmp_markdown: Path) -> None:
            write_file(path=tmp_markdown, content=self.existing_section)

            update_markdown_content_list_of_content(path=tmp_markdown)

            assert_path_content_matches_expected_content(
                path=tmp_markdown,
                expected_content=EXPECTED_MULTIPLE_CONTENT,
            )

        @pytest.mark.regression
        def test_section_updates_instead_of_prepending(self, tmp_markdown: Path) -> None:
            header = '## Pipelines Currently Managed by algo features'
            content = """```mermaid
---
title: user_features
---
graph
silver_user_check_partition ---> silver_user_daily_rollup_default_action
silver_user_check_partition ---> trial_features_default_action
silver_user_daily_rollup_default_action ---> user_features_default_action
silver_video_check_partition ---> silver_video_daily_rollup_default_action
silver_video_daily_rollup_agg_default_action ---> user_features_default_action
silver_video_daily_rollup_default_action ---> silver_video_daily_rollup_agg_default_action
silver_video_daily_rollup_default_action ---> user_features_default_action
trial_features_default_action ---> user_features_default_action
```
"""
            expected_content = f'{header}\n{content}'

            write_file(path=tmp_markdown, content=expected_content)

            utils.update_markdown_section(path=tmp_markdown, section_header=header, content=content)

            assert_path_content_matches_expected_content(
                path=tmp_markdown, expected_content=f'{expected_content}\n'
            )

    class TestMultipleExistingSections:
        other_section: str = '# Other Section'
        existing_content_other_section: str = (
            f'{SECTION_HEADER}\n existing content \n# Other Section'
        )

        def test_single_content(self, tmp_markdown: Path) -> None:
            write_file(path=tmp_markdown, content=self.existing_content_other_section)

            update_markdown_section_single_content(path=tmp_markdown)

            assert_path_content_matches_expected_content(
                path=tmp_markdown,
                expected_content=f'{EXPECTED_SINGLE_CONTENT}\n{self.other_section}',
            )

        def test_list_of_content(self, tmp_markdown: Path) -> None:
            write_file(path=tmp_markdown, content=self.existing_content_other_section)

            update_markdown_content_list_of_content(path=tmp_markdown)

            assert_path_content_matches_expected_content(
                path=tmp_markdown,
                expected_content=f'{EXPECTED_MULTIPLE_CONTENT}\n{self.other_section}',
            )
