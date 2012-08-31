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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

NameFilterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
export dataset(namesRecord) included;
export dataset(namesRecord) excluded;
    end;

filterDataset(dataset(namesRecord) ds, string search, boolean onlyOldies) := LIBRARY('NameLibrary_1_0', NameFilterLibrary(ds, search, onlyOldies));

namesTable := dataset('x',namesRecord,FLAT);
namesTable2 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

filtered := filterDataset(namesTable, 'Smith', true);
output(filtered.included,,named('Included'));

filtered2 := filterDataset(namesTable, 'Hawthorn', true);
output(filtered2.excluded,,named('Excluded'));


//Error - not all methods impemented
NameFilterLibrary_1_0(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(NameFilterLibrary)
    f := ds;
    export dataset(namesRecord) excluded := ds(surname = search);
end;


//Error - library is still abstract
NameFilterLibrary_1_1(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(NameFilterLibrary)
    f := ds;
    export dataset(namesRecord) notdefined;
    export dataset(namesRecord) included := notDefined;
    export dataset(namesRecord) excluded := ds(surname = search);
end;



