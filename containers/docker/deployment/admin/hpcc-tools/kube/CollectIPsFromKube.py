#!/usr/bin/python3
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

import os
import sys
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.sep + "..")
from CollectIPs import CollectIPs


class CollectIPsFromKube (CollectIPs):

    def __init__(self):
        '''
        constructor
        '''
        super(CollectIPs, self).__init__()


    def retrieveIPsFromCloud(self, input_fn):
        self.clean_dir(self._out_dir)
        with open(input_fn) as pods_ip_file:
           lines = pods_ip_file.readlines()

        for line in lines:
            full_node_name,node_ip = line.split(' ')
            node_ip = node_ip.rstrip('\r\n')
            node_name_items =  full_node_name.split('-')
            if ( len(node_name_items) >= 3 ) and ( node_name_items[0] != 'support' ):
                node_name = node_name_items[0] + "-" + node_name_items[1]
            elif node_name_items[0] == 'thormaster':
                node_name = node_name_items[0] + '-' + node_name_items[1]
            else:
                node_name = node_name_items[0]
            if ( 'admin' in node_name              or
                 node_name.startswith('dali')      or
                 node_name.startswith('esp')       or
                 node_name.startswith('thor')      or
                 node_name.startswith('thor_roxie') or
                 node_name.startswith('roxie')     or
                 node_name.startswith('eclcc')     or
                 node_name.startswith('scheduler') or
                 node_name.startswith('backup')    or
                 node_name.startswith('sasha')     or
                 node_name.startswith('dropzone')  or
                 node_name.startswith('support')   or
                 node_name.startswith('spark')     or
                 node_name.startswith('ldap ')     or
                 node_name.startswith('node')):
                print("node name: " + node_name)
                print("node ip: " + node_ip)
                self.write_to_file(self._out_dir, node_name, node_ip + ";")

if __name__ == '__main__':

    cip = CollectIPsFromKube()
    cip.main()
