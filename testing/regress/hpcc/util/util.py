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

import argparse
import platform
import logging

def isPositiveIntNum(string):
    for i in range(0,  len(string)):
        if (string[i] < '0') or (string[i] > '9'):
            return False
    return True

def checkPqParam(string):
    param = str(string)
    if isPositiveIntNum(string) or (string == '-1'):
        value = int(string)
    else:
        msg = "Wrong value of threadNumber parameter: '"+string+"' !"
        raise argparse.ArgumentTypeError(msg)

    return value

def getVersionNumbers():
    version = platform.python_version_tuple()
    verNum = {'main':0,  'minor':0,  'patch':0}
    if isPositiveIntNum(version[0]):
        verNum['main'] = int(version[0])
    if isPositiveIntNum(version[1]):
        verNum['minor'] = int(version[1])
    if isPositiveIntNum(version[2]):
        verNum['patch'] = int(version[2])
    return(verNum);

import json
import urllib2

gConfig = None

def setConfig(config):
    global gConfig
    gConfig = config

def queryWuid(jobname,  taskId):
    server = gConfig.server
    host = "http://"+server+"/WsWorkunits/WUQuery.json?Jobname="+jobname
    wuid="Not found"
    auth_handler = urllib2.HTTPBasicAuthHandler()
    auth_handler.add_password(realm='ESP (Authentication: LDAP server process)',
                              uri=server,
                              user=gConfig.username,
                              passwd=gConfig.password)
#    opener = urllib2.build_opener(auth_handler,  urllib2.HTTPHandler(debuglevel=1))
    opener = urllib2.build_opener(auth_handler)
    opener.add_handler(auth_handler)
    urllib2.install_opener(opener)

    try:
        response_stream = urllib2.urlopen(host)
        json_response = response_stream.read()
        resp = json.loads(json_response)
        if resp['WUQueryResponse']['NumWUs'] > 0:
            wuid= resp['WUQueryResponse']['Workunits']['ECLWorkunit'][0]['Wuid']
            state =resp['WUQueryResponse']['Workunits']['ECLWorkunit'][0]['State']
        else:
            state = jobname+' not found'
    except KeyError as ke:
        state = "Key error:"+ke.str()
        logging.debug("%3d. %s in queryWuid(%s)",  taskId,  state,  jobname)
    except urllib2.HTTPError as ex:
        state = "HTTP Error: "+ str(ex.reason)
        logging.debug("%3d. %s in queryWuid(%s)",  taskId,  state,  jobname)
    except urllib2.URLError as ex:
        state = "URL Error: "+ str(ex.reason)
        logging.error("%3d. %s in queryWuid(%s)",  taskId,  state,  jobname)
    except Exception as ex:
        state = "Unable to query "+ str(ex.reason)
        logging.debug("%3d. %s in queryWuid(%s)",  taskId,  state,  jobname)
    return {'wuid':wuid, 'state':state}

def abortWorkunit(wuid):
    host = "http://"+gConfig.server+"/WsWorkunits/WUAbort?Wuids="+wuid
    response_stream = urllib2.urlopen(host)
    json_response = response_stream.read()
    #print(json_response)
