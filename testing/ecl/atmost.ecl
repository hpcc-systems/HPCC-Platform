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

//UseStandardFiles
//UseIndexes
//nolocal
unsigned inrecs := count(DG_FlatFile) : stored('incount');

sequential(
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER),record)) != inrecs, FAIL('left outer self failed 1')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER, ATMOST(1)),record)) != inrecs, FAIL('left outer self atmost failed 2')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER),record)) != inrecs, FAIL('half-keyed Left outer failed 3')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER, ATMOST(1)),record)) != inrecs, FAIL('half-keyed Left outer atmost failed 4')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER),record)) != inrecs, FAIL('left outer failed 5')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER, ATMOST(1)),record)) != inrecs, FAIL('left outer atmost failed 6')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER),record)) != inrecs, FAIL('left outer failed 7')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT OUTER, ATMOST(1)),record)) != inrecs, FAIL('left outer atmost failed 8')),

    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY),record)) != 0, FAIL('left only self failed 1')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY, ATMOST(1)),record)) != inrecs, FAIL('left only self atmost failed 2')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY),record)) != inrecs/2, FAIL('half-keyed Left only failed 3')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY, ATMOST(1)),record)) != inrecs, FAIL('half-keyed Left only atmost failed 4')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY),record)) != 0, FAIL('left only failed 5')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY, ATMOST(1)),record)) != inrecs, FAIL('left only atmost failed 6')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY),record)) != inrecs, FAIL('left only failed 7')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, LEFT ONLY, ATMOST(1)),record)) != inrecs, FAIL('left only atmost failed 8')),

    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname),record)) != inrecs, FAIL('left inner self failed 1')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile, LEFT.dg_firstname=RIGHT.dg_firstname, ATMOST(1)),record)) != 0, FAIL('left inner self atmost failed 2')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname),record)) != inrecs/2, FAIL('half-keyed Left inner failed 3')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_indexFileEvens, LEFT.dg_firstname=RIGHT.dg_firstname, ATMOST(1)),record)) != 0, FAIL('half-keyed Left inner atmost failed 4')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname),record)) != inrecs, FAIL('left inner failed 5')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname != ''), LEFT.dg_firstname=RIGHT.dg_firstname, ATMOST(1)),record)) != 0, FAIL('left inner atmost failed 6')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname),record)) != 0, FAIL('left inner failed 7')),
    if(count(DEDUP(JOIN(DG_FlatFile, DG_FlatFile(dg_firstname = ''), LEFT.dg_firstname=RIGHT.dg_firstname, ATMOST(1)),record)) != 0, FAIL('left inner atmost failed 8')),
    output('All tests succeeded')
);
