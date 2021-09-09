import pytest

from lambda_service.helpers import parse_file_key
from lambda_service.helpers import validate_filename
from lambda_service.helpers import validate_base_filename
from lambda_service.helpers import get_data_type_from_filename
from lambda_service.helpers import get_site_from_filename
from lambda_service.helpers import get_base_filename_from_full_filename

def test_parse_file_key_good_fits():
    file_key = 'wmd-ea03-20190621-00000007-EX00.fits.bz2'
    file_parts = parse_file_key(file_key)
    assert file_parts['site'] == 'wmd'
    assert file_parts['instrument'] == 'ea03'
    assert file_parts['file_date'] == '20190621'
    assert file_parts['file_counter'] == '00000007'
    assert file_parts['data_type'] == 'EX'
    assert file_parts['reduction_level'] == '00'
    assert file_parts['file_extension'] == 'fits'
    assert file_parts['base_filename'] == 'wmd-ea03-20190621-00000007'

def test_parse_file_key_good_fits_2():
    file_key = 'wmd-ea03-20190621-00000007-e00.fits.bz2'
    file_parts = parse_file_key(file_key)
    assert file_parts['site'] == 'wmd'
    assert file_parts['instrument'] == 'ea03'
    assert file_parts['file_date'] == '20190621'
    assert file_parts['file_counter'] == '00000007'
    assert file_parts['data_type'] == 'e'
    assert file_parts['reduction_level'] == '00'
    assert file_parts['file_extension'] == 'fits'
    assert file_parts['base_filename'] == 'wmd-ea03-20190621-00000007'

def test_parse_file_key_good_jpg():
    file_key = 'wmd-ea03-20190621-00000007-EX10.jpg'
    file_parts = parse_file_key(file_key)
    assert file_parts['site'] == 'wmd'
    assert file_parts['instrument'] == 'ea03'
    assert file_parts['file_date'] == '20190621'
    assert file_parts['file_counter'] == '00000007'
    assert file_parts['data_type'] == 'EX'
    assert file_parts['reduction_level'] == '10'
    assert file_parts['file_extension'] == 'jpg'
    assert file_parts['base_filename'] == 'wmd-ea03-20190621-00000007'

def test_parse_file_key_longer_site():
    file_key = 'wmd02-ea03-20190621-00000007-EX10.jpg'
    file_parts = parse_file_key(file_key)
    assert file_parts['site'] == 'wmd02'
    assert file_parts['instrument'] == 'ea03'
    assert file_parts['file_date'] == '20190621'
    assert file_parts['file_counter'] == '00000007'
    assert file_parts['data_type'] == 'EX'
    assert file_parts['reduction_level'] == '10'
    assert file_parts['file_extension'] == 'jpg'
    assert file_parts['base_filename'] == 'wmd02-ea03-20190621-00000007'

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