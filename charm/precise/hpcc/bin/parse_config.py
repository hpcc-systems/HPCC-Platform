import sys
import yaml

config_file = sys.argv[1]
stream = file(config_file, "r")
config = yaml.load(stream)

envgen_options = [ 'roxienodes',
                   'slavesPerNode',
                   'supportnodes',
                   'thornodes',
                   'envgen-signature'
                 ]

for option in envgen_options:
  try:
      value = config["settings"][option]["value"]
  except:
      value = ""
  print (option + "=" + str(value))
