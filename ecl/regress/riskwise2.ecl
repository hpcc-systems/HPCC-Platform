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

#option ('targetClusterType', 'hthor');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
string8         dt_last_seen := '';
integer2        age := 25;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin','700117',31},
        {'Hawthorn','Mia','723213', 30},
        {'Smithe','Pru','900120', 10},
        {'X','Z'}], namesRecord);

namesRecord filter1(namesRecord le, namesRecord ri) := transform
    chooser1 := ri.dt_last_seen[1..6] > le.dt_last_seen[1..6];
    self := if(chooser1, ri, le);
end;

one_gong := rollup(namesTable, true, filter1(left, right));                     //do the actual rollup to narrow down to one record

output(one_gong, named('gong'));

x := if (exists(one_gong), one_gong, dedup(one_gong));

output(x);
