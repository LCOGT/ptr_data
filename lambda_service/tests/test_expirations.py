import pytest

from lambda_service.expirations import add_expiration_entry
from lambda_service.expirations import remove_expired_data_handler
from lambda_service.expirations import data_type_has_expiration
from lambda_service.expirations import get_image_lifespan


def test_data_type_has_expiration():
    errors = []

    with_expiration = ['EP', 'EF']
    without_expiration = ['EX', 'invalid data type']

    for dt in with_expiration:
        if not data_type_has_expiration(dt):
            errors.append(f"Failed: {dt} was not recognized as a valid expiring data_type.")
    for dt in without_expiration:
        if data_type_has_expiration(dt):
            errors.append(f"Failed: {dt} was recognized as an expiring data type, but it is not.")

    # assert no error message has been registered, else print messages
    assert not errors, "errors occured:\n{}".format("\n".join(errors))



def test_get_image_lifespan_good():
    errors = []

    # List of [filename to test, correct answer]
    filenames = [
        ['wmd-ea03-20190621-00000007-EP01.fits.bz2', 86400*7],
        ['wmd-ea03-20190621-00000007-EF01.fits.bz2', 86400],
        ['tst-ea03-20190621-00000007-EP01.fits.bz2', 300],
        ['tst-ea03-20190621-00000007-EF01.fits.bz2', 300],
    ]

    for f in filenames:
        if get_image_lifespan(f[0]) != f[1]:
            errors.append(f"Incorrect image lifespan returned for {f}.")

    # assert no error message has been registered, else print messages
    assert not errors, "errors occured:\n{}".format("\n".join(errors))


