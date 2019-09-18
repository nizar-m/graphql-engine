import ruamel.yaml
import re
import os

path_matcher = re.compile(r'\$\{([^}^{]+)\}')

def path_constructor(loader, node):
  ''' Extract the matched value, expand env variable, and replace the match '''
  value = node.value
  match = path_matcher.match(value)
  env_var = match.group()[2:-1]
  return os.environ[env_var] + value[match.end():]

def yaml_process_env_vars_conf():
    ruamel.yaml.add_implicit_resolver('!env', path_matcher, None, ruamel.yaml.RoundTripLoader)
    ruamel.yaml.add_constructor('!env', path_constructor, ruamel.yaml.RoundTripConstructor)

yaml_process_env_vars_conf()

def load(f):
    return ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)

def dump(o, stream=None):
    return ruamel.yaml.dump(o, stream, Dumper=ruamel.yaml.RoundTripDumper)
