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

person := dataset('person', { unsigned8 person_id, string1 per_sex, unsigned per_ssn, string40 per_first_name, string40 per_last_name}, thor);
myRec := record                                                         
    string15 per_last_name := person.per_last_name;
    integer4 personCount := 1;
end : deprecated('Use my new rec instead');

LnameTable := table(Person, myRec);
sortedTable := sort(LnameTable, per_last_name);

myRec xform(myRec l, myRec r) := transform
    self. personCount := l.personcount + 1;
    self := l;
end                         : deprecated('You really shouldn\'t be using this function');

XtabOut := rollup(SortedTable,
                left.per_last_name=right.per_last_name and left.personCount=right.personCount+1,
                xform(left,right)) : deprecated;
output(XtabOut);


