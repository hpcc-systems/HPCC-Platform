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

//nohthor
//nothor
//The library is defined and built in aaalibrary6.ecl

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

FilterDatasetInterface(dataset(namesRecord) ds, string search) := interface
    export dataset(namesRecord) matches;
    export dataset(namesRecord) others;
end;


filterDataset(dataset(namesRecord) ds, string search) := library('aaaLibrary6',FilterDatasetInterface(ds,search));

namesTable := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Jones','John', 44},
        {'Smith','George',75},
        {'Smith','Baby', 2}], namesRecord);

filtered := filterDataset(namesTable, 'Smith');
filtered2 := filterDataset(namesTable, 'Halliday');
filtered3 := filterDataset(namesTable, 'Tricky');

addressRecord := RECORD
unsigned            id;
dataset(namesRecord)    ds;
    END;

addressTable := dataset([
    {1, [{'Halliday','Gavin',31},{'Halliday','Liz',30}]},
    {2, [{'Jones','John', 44},
        {'Smith','George',75},
        {'Smith','Baby', 2}]}
    ], addressRecord);

logging := DATASET([{'Logging'}], {string txt});
sequential(
  output(logging,named('logging'));
  output(filtered.matches,,named('MatchSmith'));
  output(filtered2.others,,named('NotHalliday'));
  output(filtered3.others,,named('NotTricky'));
  output(addressTable, { dataset matches := filterDataset(ds, 'Halliday').matches });
  output(addressTable, { dataset others := filterDataset(ds, 'Halliday').others });
)
