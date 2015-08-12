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

#option ('globalFold', false);
//Demonstrates a cse problem.  PIPE() is done 3 times, when should only really be done once.  Problem is that
//the condition is executed in eclagent, and the conditional filenames stop the graphs being rearranged.
// We only know they can't be grouped together pretty late.  It would then require another pass to spot global cse's
// between different graphs, and make those global.  Would be possible.

#option ('targetClusterType', 'thor');

string outname := '' : stored('outname');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('out1',namesRecord,FLAT);
namesTable2 := dataset('out2',namesRecord,FLAT);

x := distribute(namesTable, random());

infile := PIPE(x, 'Do Something to it') : global;

z := table(infile,{surname});

output(z,,'out'+outname);
if (count(z) != count(namesTable2), 'Counts differ' + count(z) + ',' + count(namesTable2), 'Same');
//output( ['Counts differ' + count(z) + ',' + count(namesTable2), 'Same'][if (count(z) != count(namesTable2),1,2)]);
output(z(surname != ''),,'outx'+outname);

