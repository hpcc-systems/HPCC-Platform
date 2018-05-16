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

import argparse
import filecmp
import logging
import logging.config
import datetime
import requests.packages.urllib3
from requests import Session
from requests.auth import HTTPBasicAuth
from requests.auth import HTTPDigestAuth
from zeep import Client 
from zeep.transports import Transport
from zeep.cache import SqliteCache
from collections import namedtuple
from pathlib import Path

print('WUDetails Regression (wutest.py)')
print('--------------------------------')
print('')

argumentparser = argparse.ArgumentParser()
argumentparser.add_argument('-o', '--outdir', help='Results directory', default='results', metavar='dir')
argumentparser.add_argument('-d', '--debug', help='Enable debug', action='store_true', default=False)
argumentparser.add_argument('--logresp', help='Log wudetails responses (in results directory)', action='store_true', default=False)
argumentparser.add_argument('--logreq', help='Log wudetails requests (in results directory)', action='store_true', default=False)
argumentparser.add_argument('-n', '--lastndays', help="Use workunits from last 'n' days", type=int, default=1, metavar='days')
argumentparser.add_argument('--nosslverify', help="Disable SSL certificate verification", action='store_true', default=False)
argumentparser.add_argument('-u', '--baseurl', help="Base url for both WUQuery and WUDetails", default='http://localhost:8010', metavar='url')
argumentparser.add_argument('--user', help='Username for authentication', metavar='username')
argumentparser.add_argument('--pw', help='Password for authentication', metavar='password')
argumentparser.add_argument('--httpdigestauth', help='User HTTP digest authentication(Basic auth default)', action='store_true')
args = argumentparser.parse_args()

if (args.debug):
    loglevel = logging.DEBUG
else:
    loglevel=logging.INFO
logging.basicConfig(level=loglevel, format='[%(levelname)s] %(message)s')

keydir = 'key'
wuqueryservice_url = args.baseurl + '/WsWorkunits/WUQuery.json?ver_=1.71'
wuqueryservice_wsdl_url = args.baseurl + '/WsWorkunits/WUQuery.json?ver_=1.71&wsdl'
wudetailsservice_wsdl_url = args.baseurl + '/WsWorkunits/WUDetails.json?ver_=1.71&wsdl'

requiredJobs = ( ('childds1',      ('roxie','thor','hthor')),
                 ('dedup_all',     ('roxie','hthor')),
                 ('sort',          ('roxie','thor','hthor')),
                 ('key',           ('roxie','thor','hthor')),
                 ('dict1',         ('roxie','thor','hthor')),
                 ('indexread2-multiPart(true)',('roxie', 'thor','hthor') ) )

maskValueFields = ('Definition','SizePeakMemory', 'WhenFirstRow', 'TimeElapsed', 'TimeTotalExecute', 'TimeFirstExecute', 'TimeLocalExecute',
                   'WhenStarted', 'TimeMinLocalExecute', 'TimeMaxLocalExecute', 'TimeAvgLocalExecute', 'SkewMinLocalExecute', 'SkewMaxLocalExecute',
                   'NodeMaxLocalExecute', 'NodeMaxDiskWrites', 'NodeMaxLocalExecute', 'NodeMaxLocalExecute', 'NodeMaxSortElapsed', 'NodeMinDiskWrites',
                   'NodeMinLocalExecute', 'NodeMinLocalExecute', 'NodeMinLocalExecute', 'NodeMinSortElapsed', 'SkewMaxDiskWrites', 'SkewMaxLocalExecute',
                   'SkewMaxLocalExecute', 'SkewMaxSortElapsed', 'SkewMinDiskWrites', 'SkewMinLocalExecute', 'SkewMinLocalExecute', 'SkewMinSortElapsed',
                   'TimeAvgSortElapsed', 'TimeMaxSortElapsed', 'TimeMinSortElapsed')
maskMeasureTypes = ('ts','ns', 'skw', 'node')

requests.packages.urllib3.disable_warnings()

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
                session.auth = HTTPDigestAuth(args.user, args.pw)
            else:
                session.auth = HTTPBasicAuth(args.user, args.pw)

        transport = DebugTransport(cache=SqliteCache(), session=session)
        wuquery = Client(wuqueryservice_wsdl_url, transport=transport)
    except:
        logging.critical ('Unable to obtain WSDL from %s', wuqueryservice_wsdl_url)
        raise
   
    # Calculate date range (LastNDays not processed correctly by wuquery)
    enddate = datetime.datetime.now()
    startdate = enddate - datetime.timedelta(days=args.lastndays)
    matchedWU = {}

    logging.debug ('Gathering Workunits')
    for reqjob in requiredJobs:
        reqJobname = reqjob[0]
        reqClusters = reqjob[1]
        nextPage = 0 
        while (nextPage >=0):
            wuqueryresp = wuquery.service.WUQuery(Owner='regress',
                                                  State='completed',
                                                  PageSize=500,
                                                  PageStartFrom=nextPage,
                                                  LastNDays=args.lastndays, 
                                                  StartDate=startdate.strftime('%Y-%m-%dT00:00:00'),
                                                  EndDate=enddate.strftime('%Y-%m-%dT23:59:59'),
                                                  Jobname=reqJobname + '*',
                                                  Descending='1')
                
            try:
                nextPage = wuqueryresp['NextPage']
                workunits = wuqueryresp['Workunits']['ECLWorkunit']
            except:
                return matchedWU

            try:
                logging.debug('Workunit count: %d', len(workunits))
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
                    if ( (job == reqJobname) and (cluster in reqClusters) and (key not in matchedWU)):
                        matchedWU[key] = wu['Wuid']
            except:
                logging.error('Unexpected response from WUQuery: %s', wuqueryresp) 
                raise

    return matchedWU

def GetMissingJobCount(wu):
    missingjobs = 0
    for reqjob in requiredJobs:
        jobname = reqjob[0]
        for cluster in reqjob[1]:
            key = jobname + '_' + cluster
            if (key not in wu):
                logging.error('Missing job: %s (%s)', jobname, cluster)
                missingjobs += 1
    return missingjobs
        
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
                if ((property['Name'] in maskValueFields) or (property['Measure'] in maskMeasureTypes)):
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
    logging.critical ('Unable to obtain WSDL from ' +wudetailsservice_wsdl_url)
    raise

try:
    scopeFilter = wudetails.get_type('ns0:WUScopeFilter')
    nestedFilter = wudetails.get_type('ns0:WUNestedFilter')
    propertiesToReturn = wudetails.get_type('ns0:WUPropertiesToReturn')
    scopeOptions = wudetails.get_type('ns0:WUScopeOptions')
    propertyOptions = wudetails.get_type('ns0:WUPropertyOptions')
    extraProperties = wudetails.get_type('ns0:WUExtraProperties')
except:
    logging.critical ('WSDL different from expected')
    raise


# Generate Test cases
testCase = namedtuple('testCase', ['ScopeFilter',
                                   'NestedFilter',
                                   'PropertiesToReturn',
                                   'ScopeOptions',
                                   'PropertyOptions'])

#scopeFilter(MaxDepth='999', Scopes=set(), Ids=set(), ScopeTypes=set()),
#nestedFilter(Depth='999', ScopeTypes=set()),
#propertiesToReturn(AllProperties='1', MinVersion='0', Measure='', Properties=set(), ExtraProperties=set()),
#scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
#propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeFormatted='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
testCases = [ 
             testCase(
                 scopeFilter(MaxDepth='999'),
                 nestedFilter(),
                 propertiesToReturn(AllProperties='1'),
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
                 scopeFilter(MaxDepth='999', Scopes={'Scope':'w1:graph1'}),
                 nestedFilter(),
                 propertiesToReturn(AllProperties='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='1', ScopeTypes={'ScopeType':'graph'}),
                 nestedFilter(Depth='1'),
                 propertiesToReturn(AllProperties='1'),
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
                 scopeFilter(MaxDepth='2', Scopes={'Scope':'w1:graph1:sg1'}),
                 nestedFilter(Depth=0),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             testCase(
                 scopeFilter(MaxDepth='2', Scopes={'Scope':'w1:graph1:sg1'}),
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
    logging.debug('Executing %s',testfilename)
 
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
        logging.critical('Unable to submit WUDetails request')
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
        logging.critical('Unable to process WUDetails response')
        logging.critical('Request & response content in %s', str(errfile))
        with errfile.open(mode='w') as f:
            print ('====== Request ===========', file=f)
            print (transport.xml_request, file=f)
            print ('====== Response ==========', file=f)
            print (wuresp, file=f)
        return False

    with outfile.open(mode='w') as f:
        print (tcase, file=f)
        print (wuresp, file=f)

    if (not keyfile.exists()):       
        logging.error('FAILED %s', testfilename)
        logging.error('Missing key file %s', str(keyfile))
        return False

    # Compare actual and expectetd
    if (not filecmp.cmp(str(outfile),str(keyfile))):
        logging.error('FAILED %s', testfilename)
        logging.error('WUDetails response %s', str(outfile))
        return False
    else:
        logging.debug('PASSED %s', testfilename)
        return True

class Statistics:
    def __init__(self):
        self.successCount = 0
        self.failureCount = 0 
       
    def addCount(self, success):
        if (success):
            self.successCount += 1
        else:
            self.failureCount += 1

resultdir = Path(args.outdir)
resultdir.mkdir(parents=True, exist_ok=True)
tcasekeydir = Path(keydir)
tcasekeydir.mkdir(parents=True, exist_ok=True)

logging.info('Gathering workunits')
try:
    wu = GetTestWorkunits()
except:
    raise

logging.info('Matched job count: %d', len(wu))
if (len(wu)==0):
    logging.error('There are no matching jobs.  Has the regression suite been executed?')
    logging.error('Aborting')
    exit(1)

missingjobs = GetMissingJobCount(wu)
if (missingjobs > 0):
    logging.warning('There are %d missing jobs.  Full regression will not be executed', missingjobs)

logging.info('Executing regression test cases')
stats = Statistics()
for jobname, wuid in wu.items():
    logging.debug('Job %s (WUID %s)', jobname, wuid)

    if (jobname == 'sort_thor'):
        for index, t in enumerate(testCases):
            tcasename = 'testcase' + str(index+1)
            success = ExecTestCase(jobname, wuid, t, tcasename)
            stats.addCount(success)
    else:
        success = ExecTestCase(jobname, wuid, testCases[0], 'testcase1')
        stats.addCount(success)
logging.info('Success count: %d', stats.successCount)
logging.info('Failure count: %d', stats.failureCount)
