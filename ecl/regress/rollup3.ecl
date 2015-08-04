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


