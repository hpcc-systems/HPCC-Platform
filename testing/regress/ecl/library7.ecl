/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

//nohthor
//nothor
//The library is defined and built in aaalibrary6.ecl

idRecord := RECORD
    unsigned id;
END;

childRecord := RECORD
    unsigned5 childId;
    dataset(idRecord) ids;
END;

searchOptions := RECORD
    unsigned3 mainId;
    dataset(childRecord) children;
END;


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

FilterDatasetInterface(dataset(namesRecord) ds, searchOptions options) := interface
    export dataset(namesRecord) matches;
    export dataset(namesRecord) others;
end;


filterDataset(dataset(namesRecord) ds, searchOptions options) := library('aaaLibrary7',FilterDatasetInterface(ds,options));


mkOptions(unsigned age) := ROW({0, [{2, [{age}]}]}, searchOptions);

namesTable := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Jones','John', 44},
        {'Smith','George',75},
        {'Smith','Baby', 2}], namesRecord);

filtered1 := filterDataset(namesTable, mkOptions(30));
filtered2 := filterDataset(namesTable, mkOptions(75));

logging := DATASET([{'Logging'}], {string txt});
sequential(
  output(logging,named('logging'));
  output(filtered1.matches,,named('Liz'));
  output(filtered2.matches,,named('Gavin'));
  output(filtered2.others,,named('NotGavin'));
)
