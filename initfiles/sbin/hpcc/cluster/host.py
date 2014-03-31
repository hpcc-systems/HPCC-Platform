'''
/*#############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################ */
'''

import subprocess
import socket
import re
import logging

class Host(object):
    '''
    This class represent cluster host attributions. Currently only ip is required.
    Several static help methods are provided to create cluster hast list.
    '''
    # Default user name and passwords
    user_name = 'hpcc'
    user_passowrd = 'hpcc'
    admin_password = 'hpcc'

    logger = logging.getLogger("hpcc.cluster.Host")

    def __init__(self, ip=None):
        '''
        Constructor
        '''
        self._ip = ip
        self._host_name = None
        self._user_name = None
        self._user_password = None
        self._admin_password = None

    @property
    def ip(self):
        return self._ip

    @ip.setter
    def ip(self, value):
        self._ip = value

    @property
    def host_name(self):
        return self._host_name

    @host_name.setter
    def host_name(self, value):
        self._host_name = value

    @property
    def user_name(self):
        if self._user_name:
            return self._user_name
        else:
            return Host.user_name

    @user_name.setter
    def user_name(self, value):
        self._user_name = value


    @property
    def user_password(self):
        if self._user_password:
            return self._user_password
        else:
            return Host.user_password

    @user_password.setter
    def user_password(self, value):
        self._user_password = value



    @property
    def admin_password(self):
        if self._admin_password:
            return self._admin_password
        else:
            return Host.admin_password

    @admin_password.setter
    def admin_password(self, value):
        self._admin_password = value


    @classmethod
    def get_hosts_from_env(cls, env_xml="/etc/HPCCSystems/environment.xml",
                           hpcc_home="/opt/HPCCSystems", exclude_local=False):

        cmd = hpcc_home + "/sbin/configgen -env " + env_xml + \
              " -machines | awk -F, '{print $1} ' | sort | uniq"

        hosts = []

        try:
            process = subprocess.Popen(cmd, shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

            process.wait()
            errcode = process.returncode
            stdout, stderr = process.communicate()


            if (errcode == 0):
                ips = stdout
                for ip in ips.split():
                    ip = ip.strip()
                    if ip:
                        hosts.append( Host(ip) )
            else:
                cls.logger.error(stderr)

        except Exception as e:
            cls.logger.error(e.output)


        if exclude_local:
            return Host.exclude_local_host(hosts)
        else:
            return hosts


    @classmethod
    def get_hosts_from_file(cls, file_name, exclude_local=False):
        hosts = []
        with open(file_name) as host_file:
            for line in host_file:
                ip = line.strip()
                if ip:
                    hosts.append( Host(ip) )
        if exclude_local:
            return Host.exclude_local_host(hosts)
        else:
            return hosts

    @classmethod
    def exclude_local_host(cls, in_hosts):
        out_hosts = []
        try:
            addr_list = socket.getaddrinfo(socket.gethostname(), None)
            for host in in_hosts:
                found = False
                for addr in addr_list:
                    if addr[4][0] == host.ip:
                        found = True
                        break
                if not found:
                    out_hosts.append(host)

        except Exception as e:
            out_hosts = in_hosts

        #The above does not work if hostname doesn't match ip
        out_hosts_2 = []
        cmd = '/sbin/ifconfig -a | grep \"[[:space:]]*inet[[:space:]]\"'

        try:
            process = subprocess.Popen(cmd, shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
            process.wait()
            errcode = process.returncode

            if errcode != 0:
                return out_hosts

            inet_out, stderr = process.communicate()
            if not inet_out:
                return out_hosts

            for host in out_hosts:
                m = re.search(host.ip, inet_out)
                if not m:
                    out_hosts_2.append(host)

        except Exception as e:
            out_hosts_2 = out_hosts

        return out_hosts_2
