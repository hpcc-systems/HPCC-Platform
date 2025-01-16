/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//nothor
//nohthor

//version multiPart=false
//version multiPart=true
//version multiPart=true,variant='inplace'
//version multiPart=true,variant='default'
//version multiPart=true,variant='inplace',conditionVersion=2
//version multiPart=true,variant='inplace',conditionVersion=3
//version multiPart=true,variant='',conditionVersion=2
//version multiPart=true,variant='',conditionVersion=4

// The settings below may be useful when trying to analyse Roxie keyed join behaviour, as they will
// eliminate some wait time for an agent queue to become available

//#option('roxie:minPayloadSize', 10000)
//#option('roxie:agentThreads', 400)
//#option('roxie:prestartAgentThreads', true)

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
variant := #IFDEFINED(root.variant, 'inplace') : stored('variant');
numJoins := #IFDEFINED(root.numJoins, 1);

conditionVersion := #IFDEFINED(root.conditionVersion, 1);

#option ('allowActivityForKeyedJoin', true);
#onwarning (4523, ignore);

trueExpr := true : stored('true');

//--- end of version configuration ---

import $.setup;
files := setup.files(multiPart, false);


createSample(unsigned i, unsigned num, unsigned numRows) := FUNCTION

    //Add a keyed filter to ensure that no splitter is generated.
    //The splitter performs pathologically on roxie - it may be worth further investigation
    filtered := files.getSearchSource()(HASH32(kind, word, doc, segment, wpos) % num = i, keyed(word != ''));
    inputFile := choosen(filtered, numRows);
    keyFile1 := CASE(variant,
                    'inplace' => files.getSearchIndexVariant('inplace'),
                    'default' => files.getSearchIndexVariant('default'),
                    files.getOptSearchIndexVariant('doesnotexist')
                );
    keyFile2 := MAP(variant = 'inplace' => files.getSearchIndexVariant('inplace'),
                   variant = 'default' => files.getSearchIndexVariant('default'),
                    files.getOptSearchIndexVariant('doesnotexist')
                );
    keyFile3 := IF(variant = 'inplace', files.getSearchIndexVariant('inplace'),
                    files.getOptSearchIndexVariant('doesnotexist')
                );

    keyFile4 := MAP(variant = 'inplace' => files.getSearchIndexVariant('inplace'),
                   variant = 'default' => files.getSearchIndexVariant('default'),
                   files.getSearchIndexVariant('inplace')(false)
                );
#if (conditionVersion = 1)
    keyFile := keyFile1;
#elif (conditionVersion = 2)
    keyFile := keyFile2;
#elif (conditionVersion = 3)
    keyFile := keyFile3;
#else
    keyFile := keyFile4;
#end

    j := JOIN(inputFile, keyFile,
            (LEFT.kind = RIGHT.kind) AND
            (LEFT.word = RIGHT.word) AND
            (LEFT.doc = RIGHT.doc) AND
            (LEFT.segment = RIGHT.segment) AND
            (LEFT.wpos = RIGHT.wpos), ATMOST(10), KEYED);
    RETURN NOFOLD(j);
END;

createSamples(iters, numRows) := FUNCTIONMACRO
    expectedCount := IF(variant != '', numRows, 0);
    o := PARALLEL(
    #DECLARE (count)
    #SET (count, 0)
    #LOOP
        #IF (%count%>=iters)
            #BREAK
        #END
        output(count(createSample(%count%, iters, numRows)) = expectedCount),
        #SET (count, %count%+1)
    #END
        output('Done')
    );
    RETURN o;
ENDMACRO;

createSamples(numJoins, 60000);
