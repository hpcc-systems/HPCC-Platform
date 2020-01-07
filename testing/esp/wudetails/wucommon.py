#! /usr/bin/python3
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

import argparse
import logging
import datetime
import traceback
import requests.packages.urllib3
import sys
import inspect
from requests import Session
from zeep import Client
from zeep.transports import Transport
from pathlib import Path
from collections import namedtuple
from zeep.cache import SqliteCache

TestCase = namedtuple('testCase', ['ScopeFilter',
                                   'NestedFilter',
                                   'PropertiesToReturn',
                                   'ScopeOptions',
                                   'PropertyOptions'])

def safeMkdir(path):
    try:
        path.mkdir(parents=True)
    except FileExistsError as e:
        pass
    except (FileNotFoundError, PermissionError) as e:
        logging.error("'%s' \nExit." % (str(e)))
        exit(-1)
    except:
        print("Unexpected error:" + str(sys.exc_info()[0]) + " (line: " + str(inspect.stack()[0][2]) + ")" )
        traceback.print_stack()
        exit(-1)

class DebugTransport(Transport):
    def post(self, address, message, headers):
        self.xml_request = message.decode('utf-8')
        response = super().post(address, message, headers)
        self.response = response
        return response

class Statistics:
    def __init__(self):
        self.successCount = 0
        self.failureCount = 0

    def addCount(self, success):
        if (success):
            self.successCount += 1
        else:
            self.failureCount += 1

class WuTest:
    def connect(self):
        # Consume WSDL and generate soap structures
        try:
            session = Session()
            if (self.args.nosslverify):
                session.verify = False
                session.sslverify = False
            self.transport = DebugTransport(cache=SqliteCache(), session=session)
            self.wudetails = Client(self.wudetailsservice_wsdl_url, transport=self.transport)
        except:
            logging.critical ('Unable to connect/obtain WSDL from ' + self.wudetailsservice_wsdl_url)
            raise

    def __init__(self, maskFields=(), maskMeasureTypes=(), maskVersion=False, maskWUID=False):
        argumentparser = argparse.ArgumentParser()
        argumentparser.add_argument('-o', '--outdir', help='Results directory', default='results', metavar='dir')
        argumentparser.add_argument('-d', '--debug', help='Enable debug', action='store_true', default=False)
        argumentparser.add_argument('--logresp', help='Log wudetails responses (in results directory)', action='store_true', default=False)
        argumentparser.add_argument('--logreq', help='Log wudetails requests (in results directory)', action='store_true', default=False)
        argumentparser.add_argument('-n', '--lastndays', help="Use workunits from last 'n' days", type=int, default=1, metavar='days')
        argumentparser.add_argument('--nosslverify', help="Disable SSL certificate verification", action='store_true', default=False)
        argumentparser.add_argument('--baseurl', help="Base url for both WUQuery and WUDetails", default='http://localhost:8010', metavar='url')
        argumentparser.add_argument('--user', help='Username for authentication', metavar='username')
        argumentparser.add_argument('--pw', help='Password for authentication', metavar='password')
        argumentparser.add_argument('--httpdigestauth', help='User HTTP digest authentication(Basic auth default)', action='store_true')
        self.args = argumentparser.parse_args()

        if (self.args.debug):
            loglevel = logging.DEBUG
        else:
            loglevel=logging.INFO
        logging.basicConfig(level=loglevel, format='[%(levelname)s] %(message)s')
        requests.packages.urllib3.disable_warnings()
        self.keydir = 'key'
        self.wuqueryservice_wsdl_url = self.args.baseurl + '/WsWorkunits/WUQuery.json?ver_=1.71&wsdl'
        self.wudetailsservice_wsdl_url = self.args.baseurl + '/WsWorkunits/WUDetails.json?ver_=1.71&wsdl'
        self.outdir = self.args.outdir

        if (len(maskFields)==0 and len(maskMeasureTypes)==0 and maskWUID==False and maskVersion==False):
            self.enableMasking = False
        else:
            self.enableMasking = True

        self.maskFields = maskFields
        self.maskMeasureTypes = maskMeasureTypes
        self.maskVersion = maskVersion
        self.maskWUID = maskWUID

        self.resultdir = Path(self.args.outdir)
        safeMkdir(self.resultdir)

        self.tcasekeydir = Path(self.keydir)
        safeMkdir(self.tcasekeydir)
        try:
            self.connect()
            try:
                self.scopeFilter = self.wudetails.get_type('ns0:WUScopeFilter')
                self.nestedFilter = self.wudetails.get_type('ns0:WUNestedFilter')
                self.propertiesToReturn = self.wudetails.get_type('ns0:WUPropertiesToReturn')
                self.scopeOptions = self.wudetails.get_type('ns0:WUScopeOptions')
                self.propertyOptions = self.wudetails.get_type('ns0:WUPropertyOptions')
                self.extraProperties = self.wudetails.get_type('ns0:WUExtraProperties')
            except:
                logging.critical ('WSDL different from expected')
                raise
        except:
            sys.exit('Aborting!')

        self.wuquery = Client(self.wuqueryservice_wsdl_url, transport=self.transport)

    # Mask out fields in the response structure
    #
    def maskoutFields(self, wudetails_resp, wuid):
        try:
            if (self.maskWUID and wudetails_resp['WUID']==wuid):
                wudetails_resp['WUID'] = '{masked WUID - matches request}'

            if (self.maskVersion and wudetails_resp['MaxVersion'].isnumeric()):
                wudetails_resp['MaxVersion'] = '{masked number}'

            if (wudetails_resp['Scopes'] != None):
                for scope in wudetails_resp['Scopes']['Scope']:
                    properties = scope['Properties']
                    if (properties == None):
                        continue
                    for property in properties['Property']:
                        if ((property['Name'] in self.maskFields) or (property['Measure'] in self.maskMeasureTypes)):
                            if (property['RawValue'] != None):
                                property['RawValue'] = '{masked}'
                            if (property['Formatted'] != None):
                                property['Formatted'] = '{masked}'
                        property['Creator'] = '{masked}'
        except:
            logging.critical('Unable to process WUDetails response: %s', wuid)
            raise

    def makeWuDetailsRequest(self,testfilename,wuid,tcase):
        outfile = (self.resultdir / testfilename).with_suffix('.json')
        errfile = outfile.with_suffix('.err')

        if (outfile.exists()): outfile.unlink()
        if (errfile.exists()): errfile.unlink()

        try:
            wuresp = self.wudetails.service.WUDetails(WUID=wuid,
                                            ScopeFilter=tcase.ScopeFilter,
                                            NestedFilter=tcase.NestedFilter,
                                            PropertiesToReturn=tcase.PropertiesToReturn,
                                            ScopeOptions=tcase.ScopeOptions,
                                            PropertyOptions=tcase.PropertyOptions)
        except:
            logging.critical('Unable to submit WUDetails request: %s', testfilename)
            raise
        finally:
            if (self.args.logreq):
                reqfile = outfile.with_suffix('.req')
                try:
                    if (reqfile.exists()): reqfile.unlink()
                    with reqfile.open(mode='w') as f:
                        print (self.transport.xml_request, file=f)
                except:
                    logging.critical('Unable write logrequest to file: %s', reqfile)
                    pass

        if (self.args.logresp):
            respfile = outfile.with_suffix('.resp')
            if (respfile.exists()): respfile.unlink()
            with respfile.open(mode='w') as f:
                print (wuresp, file=f)

        if (self.enableMasking):
            self.maskoutFields(wuresp, wuid)
        return wuresp

    # Get a list of workunits that will be used for testing wudetails
    def getTestWorkunits(self, requiredJobs):
        # Calculate date range (LastNDays not processed correctly by wuquery)
        enddate = datetime.datetime.now()
        startdate = enddate - datetime.timedelta(days=self.args.lastndays)
        self.matchedWU = {}

        logging.debug ('Gathering Workunits')
        for reqjob in requiredJobs:
            reqJobname = reqjob[0]
            reqClusters = reqjob[1]
            nextPage = 0
            while (nextPage >=0):
                wuqueryresp = self.wuquery.service.WUQuery(Owner='regress',
                                                    State='completed',
                                                    PageSize=500,
                                                    PageStartFrom=nextPage,
                                                    LastNDays=self.args.lastndays,
                                                    StartDate=startdate.strftime('%Y-%m-%dT00:00:00'),
                                                    EndDate=enddate.strftime('%Y-%m-%dT23:59:59'),
                                                    Jobname=reqJobname + '*',
                                                    Descending='1')
                try:
                    nextPage = wuqueryresp['NextPage']
                    workunits = wuqueryresp['Workunits']['ECLWorkunit']
                except:
                    break

                try:
                    logging.debug('jobname %s count: %d', reqJobname,len(workunits))
                    workunits.sort(key=lambda k: k['Jobname'], reverse=True)

                    # Extract jobname from jobname with date postfix
                    for wu in workunits:
                        s = wu['Jobname'].split('-')
                        cluster = wu['Cluster']
                        if (len(s) >2):
                            sep = '-'
                            job = sep.join(s[0:len(s)-2])
                        else:
                            job = wu['Jobname']
                        key = job + '_' + cluster
                        if ( (job == reqJobname) and (cluster in reqClusters) and (key not in self.matchedWU)):
                            self.matchedWU[key] = wu['Wuid']
                except:
                    logging.error('Unexpected response from WUQuery: %s', wuqueryresp)
                    raise

        return self.matchedWU

    def getMissingJobCount(self,requiredJobs):
        missingjobcount = 0
        for reqjob in requiredJobs:
            jobname = reqjob[0]
            for cluster in reqjob[1]:
                key = jobname + '_' + cluster
                if (key not in self.matchedWU):
                    logging.error('Missing job: %s (%s)', jobname, cluster)
                    missingjobcount += 1
        return missingjobcount

    def getMatchedJobCount(self):
        return len(self.matchedWU)

