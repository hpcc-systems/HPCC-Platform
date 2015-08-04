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

rec := record
string10    id;
string10    householdid;
string4 title;
string1 sex;
string33    forename;
string33    middlename;
string33    surname;
string4 yob;
string4 salary;
string40    alias;
string20    security;
string2 numDependants;
string1 ownsRentsBoard;
string8 dateResidence;
string8 dateDeceased;
string15    driversLicence;
string25    maternalName;
string4 numPublic;
string1 numNotes;
string4 numTrade;
string10    person_sumtradeid;
end;

ds := dataset('file::127.0.0.1::c$::test_data_g2::person.d00', rec, thor);

buildindex(ds, { title, surname, forename, sex, yob, salary, 0 }, '~moxietestkey', overwrite);
