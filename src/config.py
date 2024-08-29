import yaml
import pathlib
from os import path


ROOT = pathlib.Path(__file__).parent.parent.resolve()
print(ROOT)


def join(loader, node) -> str:
    """join custom tag handler for yaml to join multiple strings.

    Args:
        loader (_type_): the yaml loader.
        node (_type_): the node to perform joining on.

    Returns:
        str: a joined string. 
    """    
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])


def load_config(path_from_root: str='config.yml') -> dict:
    """load_config loads config from yml file. 
    
    Args:
        path_from_root (str): path to the yaml configuration.

    Returns:
        dict: the config.
    """    
    loader = yaml.SafeLoader
    loader.add_constructor('!join', join)

    location = path.join(ROOT, path_from_root)

    with open(f'{location}', 'r') as stream:
        config = yaml.safe_load(stream) 
    return config
