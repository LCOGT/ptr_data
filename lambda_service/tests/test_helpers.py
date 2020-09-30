import pytest

from lambda_service.helpers import validate_filename

def test_validate_filename_good():
    good_filename = 'wmd-ea03-20190621-00000007-EX00.fits.bz2'
    assert validate_filename(good_filename)

def test_validate_filename_bad():
    bad_filename = 'bad_filename'
    with pytest.raises(AssertionError):
        validate_filename(bad_filename)