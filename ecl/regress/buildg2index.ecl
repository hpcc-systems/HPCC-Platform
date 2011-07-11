/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
