from contextlib import nullcontext as does_not_raise

from src.managed_table import utils


def test_hash_string_can_hash_string():
    string = 'abcd1234'
    with does_not_raise():
        hashed_string = utils.hash_string(string=string)
        assert isinstance(hashed_string, int)
