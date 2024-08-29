from config import load_config

def test_load_config():
    config = load_config('test/resources/test_config.yml')
    assert config == {'some': 'value'}
