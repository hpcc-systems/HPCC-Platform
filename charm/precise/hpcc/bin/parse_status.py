import sys
import os.path
import yaml
import getopt
import re
#from subprocess import call


def Usage():
    print("\nUsage parse_status.py [options]\n")
    print("  -?, --help      print help")
    print("  -f, --file      Juju status file name. This is optional")
    print("  -q, --query     Query information from juju status. Multiple")
    print("                  query can be requested and seperated by comma")
    print("  -s, --service   Charm service name. In chance deployed multiple")
    print("                  service names for difference HPCC cluster this")
    print("                  option should be provided, otherwise first service")
    print("                  name will be used.")
    print("\n")



def LoadYamlObject():
   global config
   global status_file_name

   stream = file(status_file_name, "r")
   config = yaml.load(stream)

def GetServiceName():
   global service_name
   global charm_name

   if (service_name != "" ):
      return
   pattern = re.compile(".*\/"+charm_name+"-\d+")
   for key in config["services"].keys():
      if (pattern.match(config["services"][key]["charm"])):
         service_name = key
         print("service_name=" + key)
         break

def GetNumOfUnites():
   global service_name

   if (service_name == ""):
      GetServiceName()

   if (service_name == ""):
      return

   number_of_units = len(config["services"][service_name]["units"].keys())
   print("unit_number=" + str(number_of_units))

def ParseStatus():
   global query_actions
   global query_all

   for action in query_all:
       if (action == "service"):
           GetServiceName()
       elif (action == "num_of_units"):
           GetNumOfUnites()




if __name__ == "__main__":

   charm_name= os.path.basename(
               os.path.dirname(
               os.path.dirname(
               os.path.realpath(__file__))))

   config =  None

   query_all     = [
                     "service",
                     "num_of_units",
                   ]
   query_actions = ""
   query_list = []
   service_name = ""
   status_file_name = ""

   try:
      opts, args = getopt.getopt(sys.argv[1:],":q:f:s:",
           ["help", "query", "file", "service"])

   except getopt.GetoptError as err:
      print(str(err))
      Usage()
      exit()

   for arg, value in opts:
      if arg in ("-?", "--help"):
         Usage()
         exit(0)
      elif arg in ("-q", "--query"):
         query_actions = value
      elif arg in ("-s", "--service"):
         service_name = value
      elif arg in ("-f", "--file"):
         status_file_name = value
      else:
         print("\nUnknown option: " + arg)
         Usage()
         exit(0)

   if ( query_actions != "" ):
       query_list =  query_actions.split(',')
   else:
       query_list =  query_all

   if ( status_file_name == "" ):
      status_file_name = "/tmp/juju_status.txt"
      os.system("juju status > " + status_file_name)

   LoadYamlObject()

   ParseStatus()
