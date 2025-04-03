import pytest

from quotaclimat.data_processing.mediatree.i8n.country import *

def test_validate_country_code_fra():
        france_code = validate_country_code("fra")
        assert france_code == FRANCE.code

def test_validate_country_code_invalid():
    with pytest.raises(ValueError, match="Invalid country code: nz"):
        validate_country_code("nz")

def test_get_country_from_code_fra():
        france = get_country_from_code("fra")
        assert france == FRANCE

def test_get_channels_brazil():
      os.environ['ENV'] = 'prod'
      channels = get_channels(country_code=BRAZIL.code)
      assert channels == BRAZIL.channels
      os.environ['ENV'] = 'docker'

def test_get_channels_default():
      os.environ['ENV'] = 'docker'
      channels = get_channels()
      assert channels ==  ["france2"]


def test_get_channels_default():
      os.environ['ENV'] = 'prod'
      channels = get_channels()
      assert channels ==  FRANCE.channels
      os.environ['ENV'] = 'docker'