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

//noroxie 
//doesn't make sense to read from other work units in roxie.

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable2 := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);

output(namesTable2,named('Names'));


in1 := dataset(workunit('Names'), namesRecord);
output(in1);

in2 := dataset(workunit(workunit, 'Names'), namesRecord);
output(in2);
