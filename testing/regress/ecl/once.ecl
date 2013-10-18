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
            END;

namesTable1 := dataset('namesToWatch',namesRecord,FLAT);
namesTable2 := dataset([
        {'Halliday','Gavin'},
        {'Simth','John'},
        {'Cartright','Wheely'},
        {'X','Mr'}], namesRecord);

namesTable := namesTable2;

watchNames := sort(namesTable, surname, forename) : once;

numToWatch := count(namesTable) : once;

badNumToWatch := count(watchNames) : once;

searchNames := dataset([
        {'Halliday','Gavin'},
        {'Hickland','Brenda'},
        {'Smith','Joe'},
        {'X','Z'}], namesRecord);


matches := (searchNames(exists(watchNames(searchNames.surname = watchNames.surname AND searchNames.forename = watchNames.forename))));

output(matches);
output('Matched ' + (string)count(matches) + ' of ' + (string)numToWatch + ' possible');
