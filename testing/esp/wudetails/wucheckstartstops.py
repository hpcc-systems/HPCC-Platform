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
from wucommon import TestCase

def execTestCase(jobname, wuid, tcase):
    logging.debug('Executing %s',jobname)
    wuresp = wutest.makeWuDetailsRequest(jobname, wuid, tcase)

    # Check numstop and numstarts
    if (wuresp['Scopes'] != None):
        for scope in wuresp['Scopes']['Scope']:
            properties = scope['Properties']
            if (properties == None):
                continue
            numStarts = ''
            numStops = ''
            for property in properties['Property']:
                if (property['Name'] == 'NumStarts'):
                    numStarts = property['Formatted']
                elif (property['Name'] == 'NumStops'):
                    numStops = property['Formatted']
            if (numStarts != numStops):
                logging.error('Job %s WUID %s ScopeName %s NumStarts %s NumStops %s', jobname, wuid, scope['ScopeName'], numStarts, numStops)
                return False
    return True

###############################################################################
print('wucheckstartstops.py - Check NumStarts matches NumStops')
print('-------------------------------------------------------')
print('')

requiredJobs = ( ('childds1',      ['thor']),
                 ('sort',          ['thor']),
                 ('key',           ['thor']),
                 ('dict1',         ['thor']),
                 ('indexread2-multiPart(true)',['thor']),
                 ('sets',          ['thor']),
                 ('HeadingExample',['thor']),
                 ('aaawriteresult',['thor']),
                 ('action1',       ['thor']),
                 ('action1a',      ['thor']),
                 ('action2',       ['thor']),
                 ('action4',       ['thor']),
                 ('action5',       ['thor']),
                 ('aggds1-multiPart(true)',                      ['thor']),
                 ('aggds1-multiPart(false)-useSequential(true)', ['thor']),
                 ('aggds2-multiPart(true)',                      ['thor']),
                 ('aggds2-multiPart(false)-useSequential(true)', ['thor']),
                 ('aggds3-multiPart(true)',                      ['thor']),
                 ('aggds3-multiPart(false)-useSequential(true)', ['thor']),
                 ('aggds3-keyedFilters(true)',                   ['thor']),
                 ('diskread-multiPart(false)',                   ['thor']),
                 ('diskGroupAggregate-multiPart(false)',         ['thor']),
                 ('diskAggregate-multiPart(false)',              ['thor']),
                 ('dict_once',     ['thor']),
                 ('dict_null',     ['thor']),
                 ('dict_matrix',   ['thor']),
                 ('dict_map',      ['thor']),
                 ('dict_keyed-multiPart(true)',  ['thor']),
                 ('dict_keyed-multiPart(false)', ['thor']),
                 ('dict_int',      ['thor']),
                 ('dict_indep',    ['thor']),
                 ('dict_if',       ['thor']),
                 ('dict_choose',   ['thor']),
                 ('dict_case',     ['thor']),
                 ('dict_dsout',    ['thor']),
                 ('dict_dups',     ['thor']),
                 ('dict_field',    ['thor']),
                 ('dict_field2',   ['thor']),
                 ('dict_func',     ['thor']),
                 ('dict5c',        ['thor']),
                 ('dict5b',        ['thor']),
                 ('dict5a',        ['thor']),
                 ('dict5',         ['thor']),
                 ('dict3a',        ['thor']),
                 ('dict3',         ['thor']),
                 ('dict2',         ['thor']),
                 ('dict17',        ['thor']),
                 ('dict16',        ['thor']),
                 ('dict15c-multiPart(true)',   ['thor']),
                 ('dict15c-multiPart(false)',  ['thor']),
                 ('dict15b-multiPart(true)',   ['thor']),
                 ('dict15b-multiPart(false)',  ['thor']),
                 ('dict15a-multiPart(true)',   ['thor']),
                 ('dict15a-multiPart(false)',  ['thor']),
                 ('dict15-multiPart(true)',    ['thor']),
                 ('dict15-multiPart(false)',   ['thor']),
                 ('dict12',         ['thor']),
                 ('dict11',         ['thor']),
                 ('dict10',         ['thor']),
                 ('dict1',          ['thor']),
                 ('dfsrecordof',    ['thor']),
                 ('dfsj',           ['thor']),
                 ('dfsirecordof',   ['thor']),
                 ('groupread-multiPart(true)', ['thor']),
                 ('groupread-multiPart(false)',['thor']),
                 ('groupjoin1',     ['thor']),
                 ('grouphashdedup2',['thor']),
                 ('grouphashdedup', ['thor']),
                 ('grouphashagg',   ['thor']),
                 ('groupglobal3c',  ['thor']),
                 ('groupglobal3b',  ['thor']),
                 ('groupglobal3a',  ['thor']),
                 ('groupglobal2c',  ['thor']),
                 ('groupglobal2b',  ['thor']),
                 ('groupglobal2a',  ['thor']),
                 ('groupglobal1c',  ['thor']),
                 ('groupglobal1b',  ['thor']),
                 ('groupglobal1a',  ['thor']),
                 ('groupchild',     ['thor']),
                 ('group',          ['thor']),
                 ('globals',        ['thor']),
                 ('globalmerge',    ['thor']),
                 ('globalid',       ['thor']),
                 ('globalfile',     ['thor']),
                 ('global',         ['thor']),
                 ('genjoin3',       ['thor']),
                 ('fullkeyed-multiPart(true)', ['thor']),
                 ('fullkeyed-multiPart(false)',['thor']),
                 ('full_test',      ['thor']),
                 ('fromxml5',       ['thor']),
                 ('fromjson4',      ['thor']),
                 ('formatstored',   ['thor']),
                 ('filterproject2', ['thor']),
                 ('filtergroup',    ['thor']),
                 ('fileservice',    ['thor']),
                 ('diskGroupAggregate-multiPart(false)', ['thor']),
                 ('denormalize1',   ['thor']),
                 ('dataset_transform_inline', ['thor']),
                 ('choosesets',     ['thor']),
                 ('bloom2',         ['thor']),
                 ('badindex-newIndexReadMapping(false)', ['thor']),
                 ('all_denormalize-multiPart(true)',         ['thor']))

wutest = wucommon.WuTest()

logging.info('Gathering workunits')
wu = wutest.getTestWorkunits(requiredJobs)

if (wutest.getMatchedJobCount()==0):
    logging.error('There are no matching jobs.  Has the performance regression suite been executed?')
    logging.error('Aborting')
    exit(1)

wuDetailsReq = TestCase(wutest.scopeFilter(MaxDepth='999', ScopeTypes={'ScopeType':['edge']}),
                        wutest.nestedFilter(),
                        wutest.propertiesToReturn(Properties={'Property':['NumStarts','NumStops']}),
                        wutest.scopeOptions(IncludeMatchedScopesInResults='1', IncludeScope='1', IncludeId='0', IncludeScopeType='0'),
                        wutest.propertyOptions(IncludeName='1', IncludeRawValue='0', IncludeFormatted='1', IncludeMeasure='0', IncludeCreator='0', IncludeCreatorType='0'))

logging.info('Checking NumStart and NumStop matches')
stats = wucommon.Statistics()
for jobname, wuid in wu.items():
    success = execTestCase(jobname, wuid, wuDetailsReq)
    logging.debug('Job %-33s WUID %-20s Success: %s', jobname, wuid, success)
    stats.addCount(success)

logging.info('Missing count: %d', wutest.getMissingJobCount(requiredJobs))
logging.info('Matched jobs:  %d', len(wu))
logging.info('Success count: %d', stats.successCount)
logging.info('Failure count: %d', stats.failureCount)

