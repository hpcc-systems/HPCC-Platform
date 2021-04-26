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

// In slowest systems where Regression Test Engine executed with --PQ <n> on Thor
//timeout 36000

//version tmod='Date.TestDate.TestDynamic',nothor,noroxie
//version tmod='Date.TestFormat'
//version tmod='BLAS'
//version tmod='File'
//version tmod='system'
//version tmod='uni'
//version tmod='Metaphone'
//version tmod='Crypto'
//version tmod='str'
//version tmod='Math'

//version tmod='DataPatterns'
// /version tmod='DataPatterns.TestDataPatterns'
// /version tmod='DataPatterns.TestBenford'

import ^ as root;

unsigned VERBOSE := 0;

string myMod := #IFDEFINED(root.tmod, 'Full');
#if (VERBOSE = 1)
    output(myMod, named('myMod'));
#end

#IF (myMod = 'Full' )
    // The old way, full test without versioning
    import teststd as testMod;
#ELSE
    import #expand('teststd.' + myMod) as testMod;
#END

evaluate(testMod);
output('Test std completed');
