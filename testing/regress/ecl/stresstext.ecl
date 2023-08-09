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

//version multiPart=false
//version multiPart=true
//version multiPart=false,variant='default'
//version multiPart=false,variant='inplace'
//version multiPart=false,variant='inplace_row'
//version multiPart=false,variant='inplace_lzw'
//version multiPart=false,variant='inplace_lz4hc'

// The settings below may be useful when trying to analyse Roxie keyed join behaviour, as they will
// eliminate some wait time for an agent queue to become available

//#option('roxie:minPayloadSize', 10000)
//#option('roxie:agentThreads', 400)
//#option('roxie:prestartAgentThreads', true)

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
variant := #IFDEFINED(root.variant, '');

//--- end of version configuration ---

import $.setup;
files := setup.files(multiPart, false);


createSample(unsigned i, unsigned num, unsigned numRows) := FUNCTION

    //Add a keyed filter to ensure that no splitter is generated.
    //The splitter performs pathologically on roxie - it may be worth further investigation
    filtered := files.getSearchSource()(HASH32(kind, word, doc, segment, wpos) % num = i, keyed(word != ''));
    inputFile := choosen(filtered, numRows);
    j := JOIN(inputFile, files.getSearchIndexVariant(variant),
            (LEFT.kind = RIGHT.kind) AND
            (LEFT.word = RIGHT.word) AND
            (LEFT.doc = RIGHT.doc) AND
            (LEFT.segment = RIGHT.segment) AND
            (LEFT.wpos = RIGHT.wpos), ATMOST(10));
    RETURN NOFOLD(j);
END;

createSamples(iters, numRows) := FUNCTIONMACRO
    o := PARALLEL(
    #DECLARE (count)
    #SET (count, 0)
    #LOOP
        #IF (%count%>=iters)
            #BREAK
        #END
        output(count(createSample(%count%, iters, numRows)) = numRows),
        #SET (count, %count%+1)
    #END
        output('Done')
    );
    RETURN o;
ENDMACRO;

createSamples(40, 60000);
