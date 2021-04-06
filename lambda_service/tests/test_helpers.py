import pytest

from lambda_service.helpers import validate_filename
from lambda_service.helpers import validate_base_filename
from lambda_service.helpers import get_data_type_from_filename
from lambda_service.helpers import get_site_from_filename
from lambda_service.helpers import get_base_filename_from_full_filename

def test_validate_base_filename_good():
    good_base_filename = 'tst-aa00-20201231-12345678'
    assert validate_base_filename(good_base_filename)

def test_validate_base_filename_bad():
    bad_base_filename = 'bad_filename'
    with pytest.raises(AssertionError):
        validate_filename(bad_base_filename)

def test_validate_filename_good():
    good_filename = 'wmd-ea03-20190621-00000007-EX00.fits.bz2'
    assert validate_filename(good_filename)

def test_validate_filename_bad():
    bad_filename = 'bad_filename'
    with pytest.raises(AssertionError):
        validate_filename(bad_filename)

def test_get_data_type_from_filename_good():
    good_filename = 'wmd-ea03-20190621-00000007-EX00.fits.bz2'
    data_type = get_data_type_from_filename(good_filename)
    assert data_type == "EX"

def test_get_data_type_from_filename_bad():
    bad_filename = 'bad_filename'
    with pytest.raises(AssertionError):
        get_data_type_from_filename(bad_filename)

def test_get_site_from_filename_good():
    good_filename = 'wmd-ea03-20190621-00000007-EX00.fits.bz2'
    site = get_site_from_filename(good_filename)
    print(site)
    assert site == "wmd"

def test_get_site_from_filename_bad():
    bad_filename = 'bad_filename'
    with pytest.raises(AssertionError):
        get_site_from_filename(bad_filename)

def test_get_base_filename_from_full_filename_good():
    full_filename = 'wmd-ea03-20190621-00000007-EX00.fits.bz2'
    expected_base_filename = 'wmd-ea03-20190621-00000007'
    computed_base_filename = get_base_filename_from_full_filename(full_filename)
    assert computed_base_filename == expected_base_filename

def test_get_site_from_filename_bad():
    bad_filename = 'bad_filename'
    with pytest.raises(AssertionError):
        get_base_filename_from_full_filename(bad_filename)