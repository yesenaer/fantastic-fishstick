import yaml


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


def load_config() -> dict:
    """load_config loads config from yml file. 

    Returns:
        dict: the config.
    """    
    loader = yaml.SafeLoader
    loader.add_constructor('!join', join)

    with open('./config.yml', 'r') as stream:
        config = yaml.safe_load(stream) 
    return config
