/*##############################################################################

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
############################################################################## */

//nothor

#option ('checkAsserts',false);
import $.Setup.TS;
import $.Common.TextSearch;
import $.Common.TextSearchQueries;
import $.Setup;

q1 := dataset([
            '"increase"',
            'CAPS("increase")',
            'AND(CAPS("increase"),NOCAPS("the"))',
            'AND(CAPS("increase":1),NOCAPS("the":1000))',           // forces priorities of the terms - if remote, will force seeks.
            'AND(CAPS("increase":1000),NOCAPS("the":1))',           // forces priorities of the terms - but the wrong way around

            //Use a set to ensure that the remote read covers more than one part
            'AND(SET(CAPS("zacharia","Japheth","Absalom")),NOCAPS("ark"))',

            'AND(SET(CAPS("zacharia","Japheth","Absalom":1)),NOCAPS("ark":1000))',
            'AND(SET(CAPS("zacharia","Japheth","Absalom":1000)),NOCAPS("ark":1))',

            'AND("Melchisedech","rahab")',
            'AND("sisters","brothers")',

//MORE:
// STEPPED flag on merge to give an error if input doesn't support stepping.
// What about the duplicates that can come out of the proximity operators?
// where the next on the rhs is at a compatible position, but in a different document
// What about inverse of proximity x not w/n y
// Can inverse proximity be used for sentence/paragraph.  Can we combine them so short circuited before temporaries created.
//MORE: What other boundary conditions can we think of.

                ''
            ], TextSearch.queryInputRecord);

boolean useLocal := false;
Files := Setup.Files('hthor', useLocal);
searchIndex := Files.getSearchIndex();
p := project(q1, TextSearch.doBatchExecute(searchIndex, LEFT, useLocal, 0x00000200));
output(p);
