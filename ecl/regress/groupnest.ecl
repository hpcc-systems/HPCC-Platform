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

#option ('targetClusterType', 'roxie');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

gr := global(group(namesTable, surname));

f := gr(age != 0);

summary := table(f, { cnt := count(group); surname });
summaryRecord := recordof(summary);

addressRecord := record
string              addr;
dataset(namesRecord) people;
dataset(summaryRecord) families;
        end;


addressTable := dataset([{'Here', [], []}], addressRecord);

output(project(addressTable, transform(addressRecord, self.people := group(gr); self.families := summary; self := left)));

