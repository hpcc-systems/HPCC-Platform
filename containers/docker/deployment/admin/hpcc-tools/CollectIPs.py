#!/usr/bin/python3
import sys
import getopt
import json
import os.path
from copyreg import constructor

class CollectIPs(object):
    def __init__(self):
        '''
        constructor
        '''
        self._out_dir =  "/tmp/hpcc_cluster"

    @property
    def out_dir(self):
        return self._out_dir

    @out_dir.setter
    def out_dir(self, value):
        self._out_dir = value

    def retrieveIPs(self, out_dir, input_fn):
        try:
            if out_dir:
                self._out_dir = out_dir
            if not os.path.exists(self._out_dir):
                os.makedirs(self._out_dir)

            if input_fn.lower().endswith('.json') or \
               input_fn.lower().endswith('.lst'):
                self.retrieveIPsFromCloud(input_fn)
            else:
                print("Unsupport input file extension\n")
        except Exception as e:
            raise type(e)(str(e) +
                      ' Error in retrive IPs').with_traceback(sys.exc_info()[2])

    def retrieveIPsFromCloud(self, input_fn):
        pass

    def clean_dir(self, target_dir):
        for f in os.listdir(target_dir):
            f_path = os.path.join(target_dir, f)
            if os.path.isfile(f_path):
                os.unlink(f_path)

    def write_to_file(self, base_dir, comp_type, ip):
        file_name = os.path.join(base_dir, comp_type)
        if os.path.exists(file_name):
            f_ips  = open (file_name, 'a')
        else:
            f_ips  = open (file_name, 'w')
        f_ips.write(ip + "\n")
        f_ips.close()


    def main(self):
        try:
            input_filname = ""
            opts, args = getopt.getopt(sys.argv[1:],":d:i:h",
                ["help", "ip-dir", "in-file"])

            for arg, value in opts:
                if arg in ("-?", "--help"):
                    self.usage()
                    exit(0)
                elif arg in ("-d", "--ip-dir"):
                    self.out_dir = value
                elif arg in ("-i", "--in-file"):
                    input_filename = value

            self.retrieveIPs("", input_filename)

        except getopt.GetoptError as err:
            print(str(err))
            self.usage()
            exit(0)

        except Exception as e:
            print(e)
            print("Use -h or--help to see the usage.\n");

    def usage(self):
        print("Usage CollectIPs.py [option(s)]\n")
        print(" -d --ip-dir    output pod ip directory. The default is /tmp/ips.")
        print(" -i --in-file   input docker network file in json format.")
        print(" -h --help     print this usage help.")
        print("\n");
