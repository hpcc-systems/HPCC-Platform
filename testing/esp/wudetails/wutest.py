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

import wucommon
import logging
import filecmp
from wucommon import TestCase

def execTestCase(jobname, wuid, tcase, tcasename):
    testfilename = jobname + '_' + tcasename
    logging.debug('Executing %s',testfilename)

    wuresp = wutest.makeWuDetailsRequest(testfilename, wuid, tcase)

    outfile = (wutest.resultdir / testfilename).with_suffix('.json')
    if (outfile.exists()): outfile.unlink()
    with outfile.open(mode='w') as f:
        print (tcase, file=f)
        print (wuresp, file=f)

    keyfile = (wutest.tcasekeydir / testfilename).with_suffix('.json')
    if (not keyfile.exists()):
        logging.error('Missing key file %s', str(keyfile))
        return False

    # Compare actual and expectetd
    if (not filecmp.cmp(str(outfile),str(keyfile))):
        logging.error('Regression check Failed: %s', testfilename)
        return False
    else:
        logging.debug('PASSED %s', testfilename)
        return True

###############################################################################
print('WUDetails Regression (wutest.py)')
print('--------------------------------')
print('')

requiredJobs = ( ('childds1',      ('roxie','thor','hthor')),
                 ('dedup_all',     ('roxie','hthor')),
                 ('sort',          ('roxie','thor','hthor')),
                 ('key',           ('roxie','thor','hthor')),
                 ('dict1',         ('roxie','thor','hthor')),
                 ('indexread2-multiPart(true)',('roxie', 'thor','hthor')),
                 ('sets',           ('roxie','thor','hthor')) )

maskFields = ('Definition','DefinitionList','SizePeakMemory', 'WhenFirstRow', 'TimeElapsed', 'TimeTotalExecute', 'TimeFirstExecute', 'TimeLocalExecute',
                   'WhenStarted', 'TimeMinLocalExecute', 'TimeMaxLocalExecute', 'TimeAvgLocalExecute', 'SkewMinLocalExecute', 'SkewMaxLocalExecute',
                   'NodeMaxLocalExecute', 'NodeMaxDiskWrites', 'NodeMaxLocalExecute', 'NodeMaxLocalExecute', 'NodeMaxSortElapsed', 'NodeMinDiskWrites',
                   'NodeMinLocalExecute', 'NodeMinLocalExecute', 'NodeMinLocalExecute', 'NodeMinSortElapsed', 'SkewMaxDiskWrites', 'SkewMaxLocalExecute',
                   'SkewMaxLocalExecute', 'SkewMaxSortElapsed', 'SkewMinDiskWrites', 'SkewMinLocalExecute', 'SkewMinLocalExecute', 'SkewMinSortElapsed',
                   'TimeAvgSortElapsed', 'TimeMaxSortElapsed', 'TimeMinSortElapsed')

maskMeasureTypes = ('ts','ns', 'skw', 'node')

wutest = wucommon.WuTest(maskFields, maskMeasureTypes, True, True)

scopeFilter = wutest.scopeFilter
nestedFilter = wutest.nestedFilter
propertiesToReturn = wutest.propertiesToReturn
scopeOptions = wutest.scopeOptions
propertyOptions = wutest.propertyOptions
extraProperties = wutest.extraProperties

# Test cases
#scopeFilter(MaxDepth='999', Scopes=set(), Ids=set(), ScopeTypes=set()),
#nestedFilter(Depth='999', ScopeTypes=set()),
#propertiesToReturn(AllProperties='1', MinVersion='0', Measure='', Properties=set(), ExtraProperties=set()),
#scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
#propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeFormatted='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
TestCases = [
             TestCase(
                 scopeFilter(MaxDepth='999'),
                 nestedFilter(),
                 propertiesToReturn(AllProperties='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeFormatted='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999'),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999'),
                 nestedFilter(),
                 propertiesToReturn(AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999'),
                 nestedFilter(),
                 propertiesToReturn(AllHints='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', Scopes={'Scope':'w1:graph1'}),
                 nestedFilter(),
                 propertiesToReturn(AllProperties='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='1', ScopeTypes={'ScopeType':'graph'}),
                 nestedFilter(Depth='1'),
                 propertiesToReturn(AllProperties='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'global'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'activity'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'allocator'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='2'),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions()
             ),
             TestCase(
                 scopeFilter(MaxDepth='1'),
                 nestedFilter(Depth='1'),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(),
                 propertyOptions()
             ),
             TestCase(
                 scopeFilter(MaxDepth='2'),
                 nestedFilter(),
                 propertiesToReturn(Properties={'Property':['WhenStarted','WhenCreated']}),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions()
             ),
             TestCase(
                 scopeFilter(MaxDepth='2'),
                 nestedFilter(),
                 propertiesToReturn(Measure='ts'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='2'),
                 nestedFilter(),
                 propertiesToReturn(Measure='cnt'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions()
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeScope='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeId='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='0')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeRawValue='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeMeasure='0')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeCreator='0')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':'subgraph'}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeCreatorType='0')
             ),
             TestCase(
                 scopeFilter(MaxDepth='2', Scopes={'Scope':'w1:graph1:sg1'}),
                 nestedFilter(Depth=0),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='2', Scopes={'Scope':'w1:graph1:sg1'}),
                 nestedFilter(Depth=1),
                 propertiesToReturn(Properties={'Property':['WhenStarted','WhenCreated']}, ExtraProperties={'Extra':{'scopeType':'edge','Properties':{'Property':['NumStarts','NumStops']}}}),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', PropertyFilters={'PropertyFilter':{'Name':'NumRowsProcessed','MinValue':'10000','MaxValue':'20000'}}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', PropertyFilters={'PropertyFilter':{'Name':'NumIndexSeeks','MaxValue':'3'}}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', PropertyFilters={'PropertyFilter':{'Name':'NumIndexSeeks','ExactValue':'4'}}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(MaxDepth='999', PropertyFilters={'PropertyFilter':[{'Name':'NumIndexSeeks','ExactValue':'4'},{'Name':'NumAllocations','MinValue':'5','MaxValue':'10'}]}),
                 nestedFilter(),
                 propertiesToReturn(AllStatistics='1', AllAttributes='1'),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(ScopeTypes={'ScopeType':'workflow'}, MaxDepth='999',),
                 nestedFilter(Depth='0'),
                 propertiesToReturn(AllAttributes='1', Properties=[{'Property':'IdDependencyList'}]),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
             TestCase(
                 scopeFilter(ScopeTypes={'ScopeType':'workflow'}, MaxDepth='999',),
                 nestedFilter(Depth='0'),
                 propertiesToReturn(Properties=[{'Property':'IdDependency'}]),
                 scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='1', IncludeScopeType='1'),
                 propertyOptions(IncludeName='1', IncludeRawValue='1', IncludeMeasure='1', IncludeCreator='1', IncludeCreatorType='1')
             ),
            ]

logging.info('Gathering workunits')
wu = wutest.getTestWorkunits(requiredJobs)

logging.info('Matched job count: %d', wutest.getMatchedJobCount())
if (wutest.getMatchedJobCount()==0):
    logging.error('There are no matching jobs.  Has the regression suite been executed?')
    logging.error('Aborting')
    exit(1)

missingjobs = wutest.getMissingJobCount(requiredJobs)
if (missingjobs > 0):
    logging.warning('There are %d missing jobs.  Full regression will not be executed', missingjobs)

logging.info('Executing regression test cases')
stats = wucommon.Statistics()
for jobname, wuid in wu.items():
    logging.debug('Job %s (WUID %s)', jobname, wuid)

    if (jobname == 'sort_thor'):
        for index, t in enumerate(TestCases):
            tcasename = 'testcase' + str(index+1)
            success = execTestCase(jobname, wuid, t, tcasename)
            stats.addCount(success)
    elif (jobname in ['sets_thor','sets_roxie', 'sets_hthor']):
        success = execTestCase(jobname, wuid, TestCases[30], 'testcase31')
        stats.addCount(success)
        success = execTestCase(jobname, wuid, TestCases[31], 'testcase32')
        stats.addCount(success)
    else:
        success = execTestCase(jobname, wuid, TestCases[0], 'testcase1')
        stats.addCount(success)
logging.info('Success count: %d', stats.successCount)
logging.info('Failure count: %d', stats.failureCount)
