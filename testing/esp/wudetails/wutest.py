#! /usr/bin/python3
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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
import argparse
import filecmp
import logging.config
import datetime
import requests.packages.urllib3
from requests import Session
from requests.auth import HTTPBasicAuth
from requests.auth import HTTPDigestAuth
from zeep import Client 
from zeep.transports import Transport
from zeep.cache import SqliteCache
from zeep.plugins import HistoryPlugin
from collections import namedtuple
from argparse import Namespace
from pathlib import Path
from lxml import etree

argumentparser = argparse.ArgumentParser()
argumentparser.add_argument('-o', '--outdir', help='Results directory', default='results', metavar='dir')
argumentparser.add_argument('-d', '--debug', help='Enable debug', action='store_true', default=False)
argumentparser.add_argument('--logresp', help='Log wudetails responses (in results directory)', action='store_true', default=False)
argumentparser.add_argument('--logreq', help='Log wudetails requests (in results directory)', action='store_true', default=False)
argumentparser.add_argument('-n', '--lastndays', help="Use workunits from last 'n' days", type=int, default=1, metavar='days')
argumentparser.add_argument('-t', '--testcase', help="Execute give testcase", type=int, default=0, metavar='number')
argumentparser.add_argument('--nosslverify', help="Disable SSL certificate verification", action='store_true', default=False)
argumentparser.add_argument('-u', '--baseurl', help="Base url for both WUQuery and WUDetails", default='http://localhost:8010', metavar='url')
argumentparser.add_argument('--user', help='Username for authentication', metavar='username')
argumentparser.add_argument('--pw', help='Password for authentication', metavar='password')
argumentparser.add_argument('--httpdigestauth', help='User HTTP digest authentication(Basic auth default)', action='store_true')
args = argumentparser.parse_args()

keydir = 'key'

wuqueryservice_url = args.baseurl + '/WsWorkunits/WUQuery.json?ver_=1.71'
wuqueryservice_wsdl_url = args.baseurl + '/WsWorkunits/WUQuery.json?ver_=1.71&wsdl'
wudetailsservice_wsdl_url = args.baseurl + '/WsWorkunits/WUDetails.json?ver_=1.71&wsdl'

requiredJobs = ('keyed_denormalize-multiPart(true)','selfjoinlw','sort','loopall','keydiff1','keydiff','key','kafkatest')

maskValueFields = {'WhenCreated','WhenCompiled','WhenStarted','WhenFinished','Definition',
                   'TimeElapsed','TimeMinLocalExecute','TimeMaxLocalExecute','TimeAvgLocalExecute',
                   'SkewMinLocalExecute','SkewMaxLocalExecute', 'SizePeakMemory'}

requests.packages.urllib3.disable_warnings()

if (args.debug):
    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            'verbose': {
                'format': '%(name)s: %(message)s'
            }
         },
        'handlers': {
            'console': {
                'level': 'Info',
                'class': 'logging.StreamHandler',
                'formatter': 'verbose',
            },
        },
        'loggers': {
            'zeep.transports': {
                'level': 'Info',
                'propagate': True,
                'handlers': ['console'],
            },
        }
    })

class DebugTransport(Transport):
    def post(self, address, message, headers):
        self.xml_request = message.decode('utf-8')
        response = super().post(address, message, headers)
        self.response = response
        
        return response

# Get a list of workunits that will be used for testing wudetails
#
def GetTestWorkunits():
    try:
        session = Session()
        if (args.nosslverify):
            session.verify = False
            session.sslverify = False
        if (args.pw and args.user):
            if (args.httpdigestauth):
                session.auth = HTTPBasicAuth(args.user, args.pw)

        transport = DebugTransport(cache=SqliteCache(), session=session)
        wuquery = Client(wuqueryservice_wsdl_url, transport=transport)
    except:
        print ('Error: Unable to obtain WSDL from ' + wuqueryservice_wsdl_url)
        raise
   
    # Calculate date range (LastNDays not processed correctly by wuquery)
    enddate = datetime.datetime.now()
    startdate = enddate - datetime.timedelta(days=args.lastndays)

    wuqueryresp = wuquery.service.WUQuery(Owner='regress',
                                          State='completed',
                                          PageSize=999,
                                          LastNDays=7, 
                                          StartDate=startdate.strftime('%Y-%m-%dT00:00:00'),
                                          EndDate=enddate.strftime('%Y-%m-%dT23:59:59'),
                                          Descending='1')
    matchedWU = {}

    try:
        if (('Workunits' not in wuqueryresp) or ('ECLWorkunit' not in wuqueryresp['Workunits'])):
            print ('No matching workunits found')
            return {} 

        workunits = wuqueryresp['Workunits']['ECLWorkunit']
        print ('Workunit count:', len(workunits))
        workunits.sort(key=lambda k: k['Jobname'], reverse=True)

        tmpf = Path('debug.txt')
        f = tmpf.open(mode='w')
        # Extract jobname from jobname with date postfix
        for wu in workunits:
            s = wu['Jobname'].split('-')
            cluster = wu['Cluster']
            if (len(s) >2):
                s3 = s[0:len(s)-2]
                sep = '-'
                job = sep.join(s[0:len(s)-2])
                dt = s[len(s)-2]
            else:
                job = wu['Jobname']
            if (job in requiredJobs):
                print(wu['Jobname'], cluster, file=f)
            key = job + '_' + cluster
            if (job in requiredJobs) and (not key in matchedWU):
                print('    KEEP', file=f)
                matchedWU[key] = wu['Wuid']
    except:
        print('Error unexpected response from WUQuery:', js) 
        raise

    return matchedWU

# Mask out fields in the response structure
#
def maskoutFields(wudetails_resp) :
    if (wudetails_resp['MaxVersion'].isnumeric()):
        wudetails_resp['MaxVersion'] = '{masked number}'

    if (wudetails_resp['Scopes'] != None):
        for scope in wudetails_resp['Scopes']['Scope']:
            properties = scope['Properties']
            if (properties == None):
                continue
            for property in properties['Property']:
                if (property['Name'] in maskValueFields):
                    if (property['RawValue'] != None):
                        property['RawValue'] = '{masked}'
                    if (property['Formatted'] != None):
                        property['Formatted'] = '{masked}'
            
                property['Creator'] = '{masked}'

# Main
#
# Consume WSDL and generate soap structures
try:
    session = Session()
    if (args.nosslverify):
        session.verify = False
        session.sslverify = False
    transport = DebugTransport(cache=SqliteCache(), session=session)
    wudetails = Client(wudetailsservice_wsdl_url, transport=transport)
except:
    print ('Error: Unable to obtain WSDL from ' +wudetailsservice_wsdl_url)
    raise

try:
    scopeFilter = wudetails.get_type('ns0:WUScopeFilter')
    nestedFilter = wudetails.get_type('ns0:WUNestedFilter')
    propertiesToReturn = wudetails.get_type('ns0:WUPropertiesToReturn')
    scopeOptions = wudetails.get_type('ns0:WUScopeOptions')
    propertyOptions = wudetails.get_type('ns0:WUPropertyOptions')
    extraProperties = wudetails.get_type('ns0:WUExtraProperties')
except:
    print ('Error: WSDL different from expected')
    raise


# Generate Test cases
testCase = namedtuple('testCase', ['ScopeFilter',
                                   'NestedFilter',
                                   'PropertiesToReturn',
                                   'ScopeOptions',
                                   'PropertyOptions'])

#scopeFilter(MaxDepth='999', Scopes=set(), Ids=set(), ScopeTypes=set()),
#nestedFilter(Depth='999', ScopeTypes=set()),
#propertiesToReturn(AllStatistics='1', AllAttributes='1', AllHints='1', MinVersion='0', Measure='', Properties=set(), ExtraProperties=set()),
#scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
#propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeFormatted='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
testCases = [ 
             testCase(
                 scopeFilter(MaxDepth='999'),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1', AllHints='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeFormatted='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999'),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999'),
                 nestedFilter(),
                 propertiesToReturn(AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999'),
                 nestedFilter(),
                 propertiesToReturn(AllHints='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', Scopes={'Scope':'graph1'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1', AllHints='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='1', ScopeTypes={'ScopeType':'graph'}),
                 nestedFilter(Depth='1'),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1', AllHints='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'global'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'activity'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'allocator'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='2'),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions()
             ),
             testCase(
                 scopeFilter(MaxDepth='1'),
                 nestedFilter(Depth='1'),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(),
                 propertyOptions()
             ),
             testCase(
                 scopeFilter(MaxDepth='2'),
                 nestedFilter(),
                 propertiesToReturn(Properties={'Property':['WhenStarted','WhenCreated']}),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions()
             ),
             testCase(
                 scopeFilter(MaxDepth='2'),
                 nestedFilter(),
                 propertiesToReturn(Measure='ts'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='2'),
                 nestedFilter(),
                 propertiesToReturn(Measure='cnt'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions()
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeScope='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeId='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='0')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeRawValue='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeMeasure='0')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeCreator='0')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeCreatorType='0')
             ),
             testCase(
                 scopeFilter(MaxDepth='2', Scopes={'Scope':'graph1:sg1'}),
                 nestedFilter(Depth=0),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='2', Scopes={'Scope':'graph1:sg1'}),
                 nestedFilter(Depth=1),
                 propertiesToReturn(Properties={'Property':['WhenStarted','WhenCreated']}, ExtraProperties={'Extra':{'scopeType':'edge','Properties':{'Property':['NumStarts','NumStops']}}}),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', PropertyFilters={'PropertyFilter':{'Name':'NumRowsProcessed','MinValue':'10000','MaxValue':'20000'}}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', PropertyFilters={'PropertyFilter':{'Name':'NumIndexSeeks','MaxValue':'3'}}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', PropertyFilters={'PropertyFilter':{'Name':'NumIndexSeeks','ExactValue':'4'}}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='999', PropertyFilters={'PropertyFilter':[{'Name':'NumIndexSeeks','ExactValue':'4'},{'Name':'NumAllocations','MinValue':'5','MaxValue':'10'}]}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
            ]

def ExecTestCase(jobname, wuid, tcase, tcasename):
    testfilename = jobname + '_' + tcasename
 
    keyfile = (tcasekeydir / testfilename).with_suffix('.json')
    outfile = (resultdir / testfilename).with_suffix('.json')
    errfile = outfile.with_suffix('.err')
    reqfile = outfile.with_suffix('.req')
    respfile = outfile.with_suffix('.resp')

    if (outfile.exists()): outfile.unlink()
    if (errfile.exists()): errfile.unlink()
    if (reqfile.exists()): reqfile.unlink()
    if (respfile.exists()): respfile.unlink()

    try:
        wuresp = wudetails.service.WUDetails(WUID=wuid,
                                             ScopeFilter=tcase.ScopeFilter,
                                             NestedFilter=tcase.NestedFilter,
                                             PropertiesToReturn=tcase.PropertiesToReturn,
                                             ScopeOptions=tcase.ScopeOptions,
                                             PropertyOptions=tcase.PropertyOptions)
    except:
        print('Error: Unable to submit WUDetails request')
        raise

    if (args.logreq):
        with reqfile.open(mode='w') as f:
            print (transport.xml_request, file=f)
    if (args.logresp):
        with respfile.open(mode='w') as f:
            print (wuresp, file=f)
    try:
        if (wuresp['WUID']==wuid):
            wuresp['WUID'] = '{masked WUID - matches request}'

        maskoutFields(wuresp)
    except:
        print('Error: Unable to process WUDetails response (see ' +str(errfile)+')')
        with errfile.open(mode='w') as f:
            print ('====== Request ===========', file=f)
            print (transport.xml_request, file=f)
            print ('====== Response ==========', file=f)
            print (wuresp, file=f)
        return

    with outfile.open(mode='w') as f:
        print (tcase, file=f)
        print (wuresp, file=f)

    if (keyfile.exists()):       
        # Compare actual and expectetd
        if (not filecmp.cmp(str(outfile),str(keyfile))):
            print ('\t', testfilename, ': FAILED')
        else:
            print ('\t', testfilename, ': OK')
    else:
        print ('\t', testfilename, ': FAILED - missing key file')


resultdir = Path(args.outdir)
resultdir.mkdir(parents=True, exist_ok=True)
tcasekeydir = Path(keydir)
tcasekeydir.mkdir(parents=True, exist_ok=True)

try:
    wu = GetTestWorkunits()
except:
    raise

for jobname, wuid in wu.items():
    print ('Jobname:', jobname, '(WUID:' + wuid + ')')

    if (args.testcase > 0):
        index = args.testcase-1
        if (index >= len(testCases)):
            print('Invalid test case specified')
            exit(2)
        tcasename = 'testcase' + str(index+1)
        ExecTestCase(jobname, wuid, testCases[index], tcasename)
    else:
        for index, t in enumerate(testCases):
            tcasename = 'testcase' + str(index+1)
            ExecTestCase(jobname, wuid, t, tcasename)

