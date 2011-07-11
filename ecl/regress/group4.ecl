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

#option ('optimizeGraph', false);
//#option ('unlimitedResources', true);
#option ('groupAllDistribute', true);

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

//Distribute/local sort/group
group0 := group(namesTable, forename, all);
output(group0, {count(group)});

//sort/local sort/local group
sorted1 := sort(namesTable, surname);
group1 := group(sorted1, surname, forename, all);
output(group1, {count(group)});

//sort/local sort/local group
sorted2 := sort(namesTable, surname);
group2 := group(sorted2, forename, surname, all);
output(group2, {count(group)});

//sort/local sort/local group
sorted2b := sort(namesTable, surname);
group2b := group(sorted2b, forename, surname, all);
output(group2b, {count(group)});

//distribute/local sort/local group
sorted3 := distribute(namesTable, hash(surname));
group3 := group(sorted3, forename, surname, local, all);
output(group3, {count(group)});

//distribute/local sort/local group
sorted4 := distribute(namesTable, hash(surname));
group4x := group(sorted4, forename, surname, all);
output(group4x, {count(group)});

//distribute/distribute/local sort/local group
sorted5 := distribute(namesTable, hash(age));
group5 := group(sorted5, forename, surname, all);
output(group5, {count(group)});

//sort/group - non-local
sorted6 := sort(namesTable, forename, surname, age);
group6 := group(sorted6, forename, surname, all);
output(group6, {count(group)});

