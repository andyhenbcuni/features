from unittest import mock

import pytest

from src.actions import actions


class TestCheckBQPartition:
    @mock.patch('src.actions.actions.run_bq_assertion')
    def test_raises_error_when_retry_limit_reached(self, mock_run_bq_assertion):
        mock_run_bq_assertion.side_effect = ValueError

        with pytest.raises(ValueError):
            actions.check_bq_partition(
                table_name='table_name',
                dataset='dataset',
                run_day='run_day',
                retry=1,
                retry_delay=1,
            )
