import yaml


def read_configfile(configfile):
    with open(configfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    return cfg
