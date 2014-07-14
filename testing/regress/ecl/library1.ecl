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
//nothorlcr

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;


FilterDatasetInterface(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export dataset(namesRecord) matches;
    export dataset(namesRecord) others;
end;


FilterDatasetLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(FilterDatasetInterface)
    f := ds;
    shared g := if (onlyOldies, f(age >= 65), f);
    export matches := g(surname = search);
    export others := g(surname != search);
end;


filterDataset(dataset(namesRecord) ds, string search, boolean onlyOldies) := library(internal(FilterDatasetLibrary), FilterDatasetInterface(ds, search, onlyOldies));


namesTable := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Jones','John', 44},
        {'Smith','George',75},
        {'Smith','Baby', 2}], namesRecord);

filtered := filterDataset(namesTable, 'Smith', false);
output(filtered.matches,,named('MatchSmith'));

filtered2 := filterDataset(namesTable, 'Halliday', false);
output(filtered2.others,,named('NotHalliday'));

filtered3 := filterDataset(namesTable, 'Halliday', true);
output(filtered3.others,,named('OldNotHalliday'));
